import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LeaseReconciler, Status } from '../src/reconciler.js';

const REGIONS = ['us-east', 'eu-west'];

function makeReconciler(overrides = {}) {
  return new LeaseReconciler({
    regions: REGIONS,
    replicationDelay: 10,
    tombstoneRetention: 100,
    defaultTtl: 30,
    ...overrides,
  });
}

function register(overrides = {}) {
  return {
    type: 'register',
    region: 'us-east',
    instanceId: 'svc-1',
    version: 1,
    time: 0,
    ttl: 30,
    endpoint: { host: '10.0.0.1', port: 8080 },
    ...overrides,
  };
}

test('正常续租：心跳延长租约，调用方逻辑时间决定可见性', () => {
  const r = makeReconciler();
  assert.equal(r.apply(register()).applied, true);

  // 本地立即可见，远端需等待复制延迟。
  assert.equal(r.visibleEndpoints('us-east', 1).length, 1);
  assert.equal(r.visibleEndpoints('eu-west', 1).length, 0);
  assert.equal(r.visibleEndpoints('eu-west', 10).length, 1);

  // 心跳续租到 25 + 30 = 55。
  assert.equal(
    r.apply({ type: 'heartbeat', region: 'us-east', instanceId: 'svc-1', version: 1, time: 25, ttl: 30 }).applied,
    true,
  );
  assert.equal(r.statusOf('svc-1', 50), Status.ACTIVE);
  assert.equal(r.visibleEndpoints('us-east', 50).length, 1);

  // 停止心跳后按调用方时间过期。
  assert.equal(r.statusOf('svc-1', 55), Status.EXPIRED);
  assert.equal(r.visibleEndpoints('us-east', 55).length, 0);
});

test('主动注销：产生 tombstone，同版本不能复活，高版本可重新注册', () => {
  const r = makeReconciler();
  r.apply(register());
  const res = r.apply({ type: 'deregister', region: 'us-east', instanceId: 'svc-1', version: 1, time: 5 });
  assert.equal(res.applied, true);

  assert.equal(r.statusOf('svc-1', 6), Status.DEREGISTERED);
  assert.equal(r.visibleEndpoints('us-east', 6).length, 0);

  // tombstone 保留时间输出。
  const tombs = r.tombstones(6);
  assert.equal(tombs.length, 1);
  assert.deepEqual(
    { tombstonedAt: tombs[0].tombstonedAt, retainUntil: tombs[0].retainUntil, purged: tombs[0].purged },
    { tombstonedAt: 5, retainUntil: 105, purged: false },
  );
  assert.equal(r.tombstones(106)[0].purged, true);

  // 同版本重新注册被拒绝。
  const reReg = r.apply(register({ time: 20 }));
  assert.equal(reReg.applied, false);
  assert.match(reReg.reason, /tombstoned/);

  // 更高版本允许复活。
  assert.equal(r.apply(register({ version: 2, time: 30 })).applied, true);
  assert.equal(r.statusOf('svc-1', 31), Status.ACTIVE);
});

test('崩溃过期：实例宕机后租约到期，不再作为可见端点返回', () => {
  const r = makeReconciler();
  r.apply(register({ ttl: 20 }));
  // 实例在 t=1 崩溃，不再有心跳。
  assert.equal(r.statusOf('svc-1', 19), Status.ACTIVE);
  assert.equal(r.statusOf('svc-1', 20), Status.EXPIRED);
  assert.equal(r.visibleEndpoints('us-east', 25).length, 0);

  // 过期后迟到的心跳无效，必须重新注册。
  const late = r.apply({ type: 'heartbeat', region: 'us-east', instanceId: 'svc-1', version: 1, time: 40 });
  assert.equal(late.applied, false);
  assert.match(late.reason, /expired/);

  // 过期实例可通过重新注册（同版本或更高版本）恢复。
  assert.equal(r.apply(register({ version: 1, time: 45, ttl: 30 })).applied, true);
  assert.equal(r.statusOf('svc-1', 50), Status.ACTIVE);
});

test('区域分区：复制延迟内远端仍见旧状态，恢复后收敛，且查询被标记为不安全', () => {
  const r = makeReconciler();
  r.apply(register());

  // t=5 在 us-east 注销；eu-west 延迟 10，t=12 时事件在途。
  r.apply({ type: 'deregister', region: 'us-east', instanceId: 'svc-1', version: 1, time: 5 });

  // us-east 已收敛。
  assert.equal(r.visibleEndpoints('us-east', 12).length, 0);
  // eu-west 在 t=12 还看不到注销事件（5 + 10 > 12），租约也未到期，仍见旧端点。
  assert.equal(r.visibleEndpoints('eu-west', 12).length, 1);

  // 但此时对 eu-west 的读取不安全：有在途事件。
  const safety = r.querySafety('eu-west', 12);
  assert.equal(safety.safe, false);
  assert.equal(safety.inFlight.length, 1);
  assert.equal(safety.inFlight[0].visibleAt, 15);

  // 分区恢复（复制追上）后 eu-west 收敛，查询恢复安全。
  assert.equal(r.visibleEndpoints('eu-west', 15).length, 0);
  assert.equal(r.querySafety('eu-west', 15).safe, true);

  const report = r.reconcile('eu-west', 12);
  assert.equal(report.safe, false);
  assert.equal(report.unsafeReasons.length, 1);
});

test('快照回放：较旧快照不能复活已注销实例，较新条目正常合并', () => {
  const r = makeReconciler();
  r.apply(register());
  r.apply(register({ instanceId: 'svc-2', endpoint: { host: '10.0.0.2', port: 8080 } }));
  // svc-2 在 t=20 有更新的心跳。
  r.apply({ type: 'heartbeat', region: 'us-east', instanceId: 'svc-2', version: 1, time: 20, ttl: 30 });
  // svc-1 在 t=5 注销。
  r.apply({ type: 'deregister', region: 'us-east', instanceId: 'svc-1', version: 1, time: 5 });

  // eu-west 分区恢复后回放 t=3 的旧快照（仍认为 svc-1 活跃）。
  const replay = r.apply({
    type: 'snapshot',
    region: 'eu-west',
    snapshotTime: 3,
    entries: [
      {
        instanceId: 'svc-1',
        version: 1,
        status: Status.ACTIVE,
        endpoint: { host: '10.0.0.1', port: 8080 },
        leaseExpiresAt: 33,
        lastEventTime: 3,
      },
      {
        instanceId: 'svc-3',
        version: 4,
        status: Status.ACTIVE,
        endpoint: { host: '10.0.0.3', port: 8080 },
        leaseExpiresAt: 40,
        lastEventTime: 3,
      },
    ],
  });
  assert.equal(replay.entriesApplied, 1); // 仅 svc-3 合并
  assert.equal(replay.entriesSkipped, 1);
  assert.match(replay.skipped[0].reason, /tombstone/);

  // svc-1 保持注销，未被旧快照复活。
  assert.equal(r.statusOf('svc-1', 10), Status.DEREGISTERED);
  assert.equal(r.statusOf('svc-3', 10), Status.ACTIVE);

  // 无条件"最后一条消息覆盖"被明确拒绝：svc-2 的旧快照条目同样被跳过。
  const staleEntry = r.apply({
    type: 'snapshot',
    region: 'eu-west',
    snapshotTime: 2,
    entries: [
      {
        instanceId: 'svc-2',
        version: 1,
        status: Status.ACTIVE,
        endpoint: { host: '10.9.9.9', port: 9999 },
        leaseExpiresAt: 32,
        lastEventTime: 2,
      },
    ],
  });
  assert.equal(staleEntry.entriesApplied, 0);
  assert.equal(r.instances.get('svc-2').endpoint.host, '10.0.0.2');
});

test('乱序事件：注销先于注册到达、迟到心跳与过期事件都被正确处理', () => {
  const r = makeReconciler();
  // deregister 先于 register 到达（复制乱序）。
  assert.equal(
    r.apply({ type: 'deregister', region: 'us-east', instanceId: 'svc-1', version: 1, time: 10 }).applied,
    true,
  );
  // 逻辑时间更早的 register 随后到达：被 tombstone 拒绝，实例保持注销。
  const late = r.apply(register({ time: 1 }));
  assert.equal(late.applied, false);
  assert.equal(r.statusOf('svc-1', 11), Status.DEREGISTERED);

  // 注销后的迟到心跳同样被拒绝。
  const hb = r.apply({ type: 'heartbeat', region: 'us-east', instanceId: 'svc-1', version: 1, time: 12 });
  assert.equal(hb.applied, false);
  assert.match(hb.reason, /after deregister/);

  // 正常路径中的乱序心跳：时间更旧的心跳被忽略。
  r.apply(register({ instanceId: 'svc-2', version: 1, time: 0, ttl: 30 }));
  r.apply({ type: 'heartbeat', region: 'us-east', instanceId: 'svc-2', version: 1, time: 20, ttl: 30 });
  const staleHb = r.apply({ type: 'heartbeat', region: 'us-east', instanceId: 'svc-2', version: 1, time: 15 });
  assert.equal(staleHb.applied, false);
  assert.equal(staleHb.reason, 'stale heartbeat');
  assert.equal(r.instances.get('svc-2').leaseExpiresAt, 50);
});

test('并发冲突：同版本同时间的分歧注册被标记为冲突并排除出端点列表', () => {
  const r = makeReconciler();
  r.apply(register());
  const res = r.apply(
    register({ region: 'eu-west', endpoint: { host: '10.0.0.99', port: 8080 } }),
  );
  assert.equal(res.applied, false);
  assert.equal(res.reason, 'conflict');

  const conflicts = r.conflicts();
  assert.equal(conflicts.length, 1);
  assert.equal(conflicts[0].instanceId, 'svc-1');
  // 冲突实例不再作为安全端点返回。
  assert.equal(r.visibleEndpoints('us-east', 1).length, 0);
});

test('并发注销平局：deregister 优先并记录冲突', () => {
  const r = makeReconciler();
  r.apply(register());
  const res = r.apply({ type: 'deregister', region: 'eu-west', instanceId: 'svc-1', version: 1, time: 0 });
  assert.equal(res.applied, true);
  assert.equal(res.conflicts.length, 1);
  assert.equal(r.statusOf('svc-1', 1), Status.DEREGISTERED);
});
