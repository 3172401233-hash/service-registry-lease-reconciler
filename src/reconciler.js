/**
 * 服务注册租约协调器。
 *
 * 本模块只消费注册中心产生的事件（注册 / 心跳 / 注销 / 快照），
 * 基于调用方传入的逻辑时间计算权威状态，不替代真实注册中心。
 *
 * 状态机（每个实例、按 version 维度）：
 *
 *   ABSENT  --register(v)-------------> ACTIVE      （无有效 tombstone 或 v 大于 tombstone 版本）
 *   ACTIVE  --heartbeat---------------> ACTIVE      （续租，延长 leaseExpiresAt）
 *   ACTIVE  --deregister--------------> DEREGISTERED（产生 tombstone）
 *   ACTIVE  --now > leaseExpiresAt----> EXPIRED     （派生状态，按读取时的逻辑时间计算）
 *   EXPIRED --register(v' >= v)-------> ACTIVE      （重新注册）
 *   EXPIRED --deregister--------------> DEREGISTERED
 *   DEREGISTERED --register(v' > v)---> ACTIVE      （换版本复活；同版本拒绝，防止旧快照复活）
 *
 * 其余迁移一律拒绝并说明原因。
 *
 * 并发与乱序规则：
 *  - 事件按 (version, time) 做 last-writer-wins；严格更旧的事件视为 stale 被拒绝。
 *  - (version, time) 相同但内容不同的并发事件记为冲突；deregister 在平局时优先（更安全）。
 *  - tombstone 在保留期内阻止同版本或更旧版本的注册 / 快照条目复活实例。
 */

export const Status = Object.freeze({
  ACTIVE: 'ACTIVE',
  EXPIRED: 'EXPIRED',
  DEREGISTERED: 'DEREGISTERED',
});

export const EventType = Object.freeze({
  REGISTER: 'register',
  HEARTBEAT: 'heartbeat',
  DEREGISTER: 'deregister',
  SNAPSHOT: 'snapshot',
});

const EVENT_PRIORITY = Object.freeze({
  [EventType.DEREGISTER]: 3,
  [EventType.REGISTER]: 2,
  [EventType.HEARTBEAT]: 1,
});

function normalizeDelay(replicationDelay) {
  if (typeof replicationDelay === 'function') return replicationDelay;
  if (typeof replicationDelay === 'number') return () => replicationDelay;
  if (replicationDelay && typeof replicationDelay === 'object') {
    return (from, to) => {
      const key = `${from}->${to}`;
      if (key in replicationDelay) return replicationDelay[key];
      if ('*' in replicationDelay) return replicationDelay['*'];
      return 0;
    };
  }
  return () => 0;
}

function assertEvent(event) {
  if (!event || typeof event !== 'object') throw new TypeError('event must be an object');
  if (!Object.values(EventType).includes(event.type)) {
    throw new TypeError(`unknown event type: ${event.type}`);
  }
  if (event.type !== EventType.SNAPSHOT) {
    for (const field of ['region', 'instanceId', 'version', 'time']) {
      if (event[field] === undefined) throw new TypeError(`event missing field: ${field}`);
    }
  } else {
    for (const field of ['region', 'snapshotTime', 'entries']) {
      if (event[field] === undefined) throw new TypeError(`snapshot missing field: ${field}`);
    }
  }
}

export class LeaseReconciler {
  /**
   * @param {object} options
   * @param {string[]} options.regions 已知区域列表
   * @param {number|object|function} options.replicationDelay 区域间复制延迟（逻辑时间单位）
   * @param {number} options.tombstoneRetention tombstone 保留时长（应大于最大复制延迟）
   * @param {number} [options.defaultTtl] 事件未携带 ttl 时使用的默认租约时长
   */
  constructor({ regions = [], replicationDelay = 0, tombstoneRetention, defaultTtl = 30 } = {}) {
    if (tombstoneRetention === undefined) {
      throw new TypeError('tombstoneRetention is required');
    }
    this.regions = [...regions];
    this.delay = normalizeDelay(replicationDelay);
    this.tombstoneRetention = tombstoneRetention;
    this.defaultTtl = defaultTtl;
    /** @type {Map<string, object>} instanceId -> 记录 */
    this.instances = new Map();
    /** 已应用事件的完整日志（用于检测在途事件与查询安全性）。 */
    this.eventLog = [];
  }

  /**
   * 应用一条事件。返回 { applied, reason?, conflicts? }。
   */
  apply(event) {
    assertEvent(event);
    if (event.type === EventType.SNAPSHOT) return this.#applySnapshot(event);
    this.eventLog.push(event);
    switch (event.type) {
      case EventType.REGISTER:
        return this.#applyRegister(event);
      case EventType.HEARTBEAT:
        return this.#applyHeartbeat(event);
      case EventType.DEREGISTER:
        return this.#applyDeregister(event);
      default:
        throw new TypeError(`unsupported event type: ${event.type}`);
    }
  }

  #getOrCreate(instanceId) {
    let rec = this.instances.get(instanceId);
    if (!rec) {
      rec = {
        instanceId,
        version: -1,
        status: null,
        endpoint: null,
        homeRegion: null,
        leaseExpiresAt: -Infinity,
        lastEventTime: -Infinity,
        lastEventType: null,
        lastEventRegion: null,
        tombstone: null,
        conflict: null,
      };
      this.instances.set(instanceId, rec);
    }
    return rec;
  }

  #isStale(rec, event) {
    if (event.version < rec.version) return true;
    if (event.version === rec.version && event.time < rec.lastEventTime) return true;
    return false;
  }

  #isConcurrentDivergent(rec, event) {
    return (
      event.version === rec.version &&
      event.time === rec.lastEventTime &&
      (event.type !== rec.lastEventType ||
        (event.endpoint !== undefined &&
          rec.endpoint !== null &&
          JSON.stringify(event.endpoint) !== JSON.stringify(rec.endpoint)))
    );
  }

  #recordConflict(rec, event, reason) {
    const conflict = {
      instanceId: rec.instanceId,
      reason,
      version: event.version,
      time: event.time,
      events: [
        { type: rec.lastEventType, region: rec.lastEventRegion, endpoint: rec.endpoint },
        { type: event.type, region: event.region, endpoint: event.endpoint ?? null },
      ],
    };
    rec.conflict = conflict;
    return conflict;
  }

  #applyRegister(event) {
    const rec = this.#getOrCreate(event.instanceId);
    const tombstone = rec.tombstone;
    if (tombstone && event.version <= tombstone.version) {
      return {
        applied: false,
        reason: `rejected: instance ${event.instanceId} version ${event.version} is tombstoned ` +
          `(tombstone version ${tombstone.version}); reincarnation requires a higher version`,
      };
    }
    if (this.#isConcurrentDivergent(rec, event)) {
      const conflict = this.#recordConflict(rec, event, 'concurrent divergent register');
      return { applied: false, reason: 'conflict', conflicts: [conflict] };
    }
    if (this.#isStale(rec, event)) {
      return { applied: false, reason: 'stale register: older than last applied event' };
    }
    rec.version = event.version;
    rec.status = Status.ACTIVE;
    rec.endpoint = event.endpoint ?? rec.endpoint;
    rec.homeRegion = event.region;
    rec.leaseExpiresAt = event.time + (event.ttl ?? this.defaultTtl);
    rec.lastEventTime = event.time;
    rec.lastEventType = event.type;
    rec.lastEventRegion = event.region;
    rec.tombstone = null;
    return { applied: true };
  }

  #applyHeartbeat(event) {
    const rec = this.instances.get(event.instanceId);
    if (!rec || rec.status === null) {
      return { applied: false, reason: 'heartbeat for unknown instance; register first' };
    }
    if (rec.status === Status.DEREGISTERED) {
      return {
        applied: false,
        reason: 'heartbeat after deregister rejected; instance must re-register with a higher version',
      };
    }
    if (event.version !== rec.version) {
      return {
        applied: false,
        reason: `heartbeat version ${event.version} does not match current version ${rec.version}`,
      };
    }
    if (event.time <= rec.lastEventTime) {
      return { applied: false, reason: 'stale heartbeat' };
    }
    if (event.time >= rec.leaseExpiresAt) {
      return {
        applied: false,
        reason: 'lease already expired at heartbeat time; instance must re-register',
      };
    }
    rec.status = Status.ACTIVE;
    rec.leaseExpiresAt = Math.max(rec.leaseExpiresAt, event.time + (event.ttl ?? this.defaultTtl));
    rec.lastEventTime = event.time;
    rec.lastEventType = event.type;
    rec.lastEventRegion = event.region;
    return { applied: true };
  }

  #applyDeregister(event) {
    const rec = this.#getOrCreate(event.instanceId);
    if (this.#isConcurrentDivergent(rec, event)) {
      // 平局时 deregister 优先（更安全），但仍记录冲突。
      const conflict = this.#recordConflict(rec, event, 'concurrent deregister vs other event');
      this.#tombstone(rec, event);
      return { applied: true, conflicts: [conflict] };
    }
    if (this.#isStale(rec, event)) {
      return { applied: false, reason: 'stale deregister: older than last applied event' };
    }
    this.#tombstone(rec, event);
    return { applied: true };
  }

  #tombstone(rec, event) {
    rec.version = Math.max(rec.version, event.version);
    rec.status = Status.DEREGISTERED;
    rec.leaseExpiresAt = -Infinity;
    rec.lastEventTime = event.time;
    rec.lastEventType = event.type;
    rec.lastEventRegion = event.region;
    rec.tombstone = {
      version: event.version,
      time: event.time,
      retainUntil: event.time + this.tombstoneRetention,
    };
  }

  /**
   * 应用区域快照（分区恢复后的状态回放）。
   * 逐条目按 (version, lastEventTime) 比较，仅应用更新的条目；
   * 本地 tombstone 版本 >= 条目版本时拒绝复活，防止较旧快照覆盖已注销状态。
   */
  #applySnapshot(event) {
    const result = { applied: true, entriesApplied: 0, entriesSkipped: 0, skipped: [] };
    for (const entry of event.entries) {
      const rec = this.#getOrCreate(entry.instanceId);
      const entryTime = entry.lastEventTime ?? event.snapshotTime;
      if (rec.tombstone && entry.version <= rec.tombstone.version && entry.status !== Status.DEREGISTERED) {
        result.entriesSkipped += 1;
        result.skipped.push({
          instanceId: entry.instanceId,
          reason: 'snapshot entry blocked by local tombstone; refusing to resurrect deregistered instance',
        });
        continue;
      }
      if (entry.version < rec.version ||
          (entry.version === rec.version && entryTime <= rec.lastEventTime)) {
        result.entriesSkipped += 1;
        result.skipped.push({ instanceId: entry.instanceId, reason: 'snapshot entry is stale' });
        continue;
      }
      rec.version = entry.version;
      rec.status = entry.status === Status.DEREGISTERED ? Status.DEREGISTERED : Status.ACTIVE;
      rec.endpoint = entry.endpoint ?? rec.endpoint;
      rec.homeRegion = entry.homeRegion ?? event.region;
      rec.leaseExpiresAt = entry.status === Status.DEREGISTERED ? -Infinity : entry.leaseExpiresAt;
      rec.lastEventTime = entryTime;
      rec.lastEventType = EventType.SNAPSHOT;
      rec.lastEventRegion = event.region;
      if (entry.status === Status.DEREGISTERED) {
        rec.tombstone = {
          version: entry.version,
          time: entryTime,
          retainUntil: entryTime + this.tombstoneRetention,
        };
      }
      // 将快照条目作为事实写入日志，使区域投影能感知快照带来的状态。
      this.eventLog.push({
        type: entry.status === Status.DEREGISTERED ? EventType.DEREGISTER : EventType.REGISTER,
        region: event.region,
        instanceId: entry.instanceId,
        version: entry.version,
        time: entryTime,
        ttl: entry.status === Status.DEREGISTERED ? 0 : entry.leaseExpiresAt - entryTime,
        endpoint: entry.endpoint ?? null,
        viaSnapshot: true,
      });
      result.entriesApplied += 1;
    }
    return result;
  }

  /**
   * 实例在指定逻辑时间的派生状态（租约过期由调用方时间计算）。
   */
  statusOf(instanceId, now) {
    const rec = this.instances.get(instanceId);
    if (!rec || rec.status === null) return null;
    if (rec.status === Status.DEREGISTERED) return Status.DEREGISTERED;
    return rec.leaseExpiresAt > now ? Status.ACTIVE : Status.EXPIRED;
  }

  /**
   * 区域投影：只折叠在逻辑时间 now 之前已复制到 region 的事实，
   * 得出该区域视角下实例的状态。分区期间远端区域因此仍看到旧状态，
   * 而不是被全局最后一条事件无条件覆盖。
   */
  #project(instanceId, region, now) {
    let latest = null;
    let endpoint = null;
    let endpointKey = null;
    let leaseExpiresAt = -Infinity;
    for (const fact of this.eventLog) {
      if (fact.instanceId !== instanceId) continue;
      if (fact.region !== region && fact.time + this.delay(fact.region, region) > now) continue;
      if (fact.type !== EventType.DEREGISTER) {
        leaseExpiresAt = Math.max(leaseExpiresAt, fact.time + (fact.ttl ?? this.defaultTtl));
      }
      const key = [fact.version, fact.time, EVENT_PRIORITY[fact.type]];
      if (
        !latest ||
        key[0] > latest._key[0] ||
        (key[0] === latest._key[0] &&
          (key[1] > latest._key[1] ||
            (key[1] === latest._key[1] && key[2] > latest._key[2])))
      ) {
        latest = { ...fact, _key: key };
      }
      if (fact.type === EventType.REGISTER && fact.endpoint) {
        if (!endpointKey || key[0] > endpointKey[0] ||
            (key[0] === endpointKey[0] && key[1] >= endpointKey[1])) {
          endpoint = fact.endpoint;
          endpointKey = key;
        }
      }
    }
    if (!latest) return null;
    const status = latest.type === EventType.DEREGISTER
      ? Status.DEREGISTERED
      : leaseExpiresAt > now
        ? Status.ACTIVE
        : Status.EXPIRED;
    return { status, endpoint, leaseExpiresAt, version: latest.version };
  }

  /**
   * 某区域在指定逻辑时间可见的活跃端点。
   * 冲突实例被排除（无法确定权威状态，返回给调用方不安全）。
   */
  visibleEndpoints(region, now) {
    const endpoints = [];
    for (const rec of this.instances.values()) {
      if (rec.conflict) continue;
      const projection = this.#project(rec.instanceId, region, now);
      if (!projection || projection.status !== Status.ACTIVE) continue;
      endpoints.push({
        instanceId: rec.instanceId,
        version: projection.version,
        endpoint: projection.endpoint ?? rec.endpoint,
        homeRegion: rec.homeRegion,
        leaseExpiresAt: projection.leaseExpiresAt,
      });
    }
    return endpoints.sort((a, b) => a.instanceId.localeCompare(b.instanceId));
  }

  /**
   * 当前 tombstone 列表及保留截止时间；now 超过 retainUntil 的标记为 purged。
   */
  tombstones(now) {
    const result = [];
    for (const rec of this.instances.values()) {
      if (!rec.tombstone) continue;
      result.push({
        instanceId: rec.instanceId,
        version: rec.tombstone.version,
        tombstonedAt: rec.tombstone.time,
        retainUntil: rec.tombstone.retainUntil,
        purged: now > rec.tombstone.retainUntil,
      });
    }
    return result.sort((a, b) => a.instanceId.localeCompare(b.instanceId));
  }

  /**
   * 当前冲突实例列表。
   */
  conflicts() {
    const result = [];
    for (const rec of this.instances.values()) {
      if (rec.conflict) result.push(rec.conflict);
    }
    return result.sort((a, b) => a.instanceId.localeCompare(b.instanceId));
  }

  /**
   * 评估一次读取是否安全：若存在逻辑时间不晚于 now、但尚未复制到
   * 读取区域的在途事件，则读取可能基于过期数据，不能安全返回。
   */
  querySafety(region, now) {
    const inFlight = this.eventLog.filter(
      (event) =>
        event.region !== region &&
        event.time <= now &&
        event.time + this.delay(event.region, region) > now,
    );
    if (inFlight.length === 0) return { safe: true, inFlight: [] };
    return {
      safe: false,
      inFlight: inFlight.map((event) => ({
        type: event.type,
        instanceId: event.instanceId,
        region: event.region,
        time: event.time,
        visibleAt: event.time + this.delay(event.region, region),
      })),
      reason: 'replication in flight: events logically before the read time have not reached this region',
    };
  }

  /**
   * 汇总一次区域读取：可见端点、tombstone、冲突实例与查询安全性。
   */
  reconcile(region, now) {
    const safety = this.querySafety(region, now);
    return {
      region,
      now,
      endpoints: this.visibleEndpoints(region, now),
      tombstones: this.tombstones(now),
      conflicts: this.conflicts(),
      safe: safety.safe,
      unsafeReasons: safety.safe ? [] : [safety.reason],
      inFlightEvents: safety.inFlight,
    };
  }
}
