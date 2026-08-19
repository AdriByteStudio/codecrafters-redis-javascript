const fs = require("fs");
const path = require("path");
const net = require("net");

const store = new Map();
const blockedClients = new Map();
const blockedXReaders = [];
const keyVersions = new Map();
const replicaConnections = new Set();
const pendingWaits = new Set();
const channelSubscribers = new Map();
let isReplayingAof = false;
const serverRole = process.argv.includes("--replicaof") ? "slave" : "master";
const masterReplicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
let masterReplicationOffset = 0;
const emptyRdbFile = Buffer.from(
  "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==",
  "base64",
);
const replicaOfIndex = process.argv.indexOf("--replicaof");
const replicaOf = replicaOfIndex === -1 ? "" : String(process.argv[replicaOfIndex + 1] ?? "");
const [masterHost, masterPort] = replicaOf.trim().split(/\s+/);
const dirIndex = process.argv.indexOf("--dir");
const rdbDirectory = dirIndex === -1 ? process.cwd() : String(process.argv[dirIndex + 1] ?? "");
const dbFilenameIndex = process.argv.indexOf("--dbfilename");
const rdbFilename = dbFilenameIndex === -1 ? "" : String(process.argv[dbFilenameIndex + 1] ?? "");
function readOption(name, defaultValue) {
  const index = process.argv.indexOf(`--${name}`);
  return index === -1 ? defaultValue : String(process.argv[index + 1] ?? "");
}

const configuration = {
  dir: rdbDirectory,
  dbfilename: rdbFilename,
  appendonly: readOption("appendonly", "no"),
  appenddirname: readOption("appenddirname", "appendonlydir"),
  appendfilename: readOption("appendfilename", "appendonly.aof"),
  appendfsync: readOption("appendfsync", "everysec"),
};

function markKeyModified(key) {
  const version = keyVersions.get(key) ?? 0;
  keyVersions.set(key, version + 1);
}

const MIN_LATITUDE = -85.05112878;
const MAX_LATITUDE = 85.05112878;
const MIN_LONGITUDE = -180;
const MAX_LONGITUDE = 180;
const LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE;

function spreadInt32ToInt64(v) {
  let b = BigInt(v) & 0xFFFFFFFFn;
  b = (b | (b << 16n)) & 0x0000FFFF0000FFFFn;
  b = (b | (b << 8n)) & 0x00FF00FF00FF00FFn;
  b = (b | (b << 4n)) & 0x0F0F0F0F0F0F0F0Fn;
  b = (b | (b << 2n)) & 0x3333333333333333n;
  b = (b | (b << 1n)) & 0x5555555555555555n;
  return b;
}

function interleave(x, y) {
  const bx = spreadInt32ToInt64(x);
  const by = spreadInt32ToInt64(y);
  return Number(bx | (by << 1n));
}

function encodeGeoCoordinate(longitude, latitude) {
  const normalizedLatitude = Math.trunc((2 ** 26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE);
  const normalizedLongitude = Math.trunc((2 ** 26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE);
  return interleave(normalizedLatitude, normalizedLongitude);
}

function compactInt64ToInt32(v) {
  let b = BigInt(v) & 0x5555555555555555n;
  b = (b | (b >> 1n)) & 0x3333333333333333n;
  b = (b | (b >> 2n)) & 0x0F0F0F0F0F0F0F0Fn;
  b = (b | (b >> 4n)) & 0x00FF00FF00FF00FFn;
  b = (b | (b >> 8n)) & 0x0000FFFF0000FFFFn;
  b = (b | (b >> 16n)) & 0x00000000FFFFFFFFn;
  return Number(b);
}

function decodeGeoCoordinate(score) {
  const bScore = BigInt(score);
  const x = compactInt64ToInt32(bScore);
  const y = compactInt64ToInt32(bScore >> 1n);

  const latMin = MIN_LATITUDE + LATITUDE_RANGE * (x / (2 ** 26));
  const latMax = MIN_LATITUDE + LATITUDE_RANGE * ((x + 1) / (2 ** 26));
  const lonMin = MIN_LONGITUDE + LONGITUDE_RANGE * (y / (2 ** 26));
  const lonMax = MIN_LONGITUDE + LONGITUDE_RANGE * ((y + 1) / (2 ** 26));

  return {
    longitude: (lonMin + lonMax) / 2,
    latitude: (latMin + latMax) / 2,
  };
}

function haversine(lat1, lon1, lat2, lon2) {
  const R = 6372797.560856;
  const toRad = (deg) => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function readRdbLength(buffer, offset) {
  const firstByte = buffer[offset];
  const encoding = firstByte >> 6;

  if (encoding === 0) {
    return { value: firstByte & 0x3f, nextOffset: offset + 1 };
  }

  if (encoding === 1) {
    return { value: ((firstByte & 0x3f) << 8) | buffer[offset + 1], nextOffset: offset + 2 };
  }

  if (encoding === 2) {
    return { value: buffer.readUInt32BE(offset + 1), nextOffset: offset + 5 };
  }

  return { encoding: firstByte & 0x3f, nextOffset: offset + 1 };
}

function readRdbString(buffer, offset) {
  const length = readRdbLength(buffer, offset);
  if (length.encoding === 0) {
    return { value: String(buffer.readInt8(length.nextOffset)), nextOffset: length.nextOffset + 1 };
  }

  if (length.encoding === 1) {
    return { value: String(buffer.readInt16LE(length.nextOffset)), nextOffset: length.nextOffset + 2 };
  }

  if (length.encoding === 2) {
    return { value: String(buffer.readInt32LE(length.nextOffset)), nextOffset: length.nextOffset + 4 };
  }

  return {
    value: buffer.toString("utf8", length.nextOffset, length.nextOffset + length.value),
    nextOffset: length.nextOffset + length.value,
  };
}

function loadRdbFile() {
  if (!rdbDirectory || !rdbFilename) {
    return;
  }

  const rdbPath = path.join(rdbDirectory, rdbFilename);
  if (!fs.existsSync(rdbPath)) {
    return;
  }

  const buffer = fs.readFileSync(rdbPath);
  let offset = 9;
  let expiresAt = null;

  while (offset < buffer.length) {
    const opcode = buffer[offset];
    offset += 1;

    if (opcode === 0xff) {
      return;
    }

    if (opcode === 0xfa) {
      offset = readRdbString(buffer, offset).nextOffset;
      offset = readRdbString(buffer, offset).nextOffset;
      continue;
    }

    if (opcode === 0xfe) {
      offset = readRdbLength(buffer, offset).nextOffset;
      continue;
    }

    if (opcode === 0xfb) {
      offset = readRdbLength(buffer, offset).nextOffset;
      offset = readRdbLength(buffer, offset).nextOffset;
      continue;
    }

    if (opcode === 0xfc) {
      expiresAt = Number(buffer.readBigUInt64LE(offset));
      offset += 8;
      continue;
    }

    if (opcode === 0xfd) {
      expiresAt = buffer.readUInt32LE(offset) * 1000;
      offset += 4;
      continue;
    }

    if (opcode !== 0) {
      return;
    }

    const key = readRdbString(buffer, offset);
    const value = readRdbString(buffer, key.nextOffset);
    store.set(key.value, { value: value.value, expiresAt });
    expiresAt = null;
    offset = value.nextOffset;
  }
}

function countAcknowledgedReplicas(offset) {
  let count = 0;
  for (const replicaConnection of replicaConnections) {
    if ((replicaConnection.replicationOffset ?? 0) >= offset) {
      count += 1;
    }
  }
  return count;
}

function completeWait(waiting) {
  clearTimeout(waiting.timeoutId);
  pendingWaits.delete(waiting);
  waiting.connection.write(serializeInteger(countAcknowledgedReplicas(waiting.targetOffset)));
}

function resolveAcknowledgedWaits() {
  for (const waiting of [...pendingWaits]) {
    if (countAcknowledgedReplicas(waiting.targetOffset) >= waiting.requiredReplicas) {
      completeWait(waiting);
    }
  }
}

function getActiveAofPath() {
  const appendOnlyDirectory = path.join(configuration.dir, configuration.appenddirname);
  const manifestPath = path.join(appendOnlyDirectory, `${configuration.appendfilename}.manifest`);
  const manifest = fs.readFileSync(manifestPath, "utf8");
  const entry = manifest.match(/^file\s+(\S+)\s+seq\s+\d+\s+type\s+i$/m);
  return path.join(appendOnlyDirectory, entry ? entry[1] : `${configuration.appendfilename}.1.incr.aof`);
}

function appendAofCommand(command) {
  if (configuration.appendonly !== "yes" || isReplayingAof) {
    return;
  }

  fs.appendFileSync(getActiveAofPath(), command);
}

function replayAofFile() {
  if (configuration.appendonly !== "yes") {
    return;
  }

  const aofPath = getActiveAofPath();
  if (!fs.existsSync(aofPath)) {
    return;
  }

  const transactionState = { active: false, commands: [], watchedKeys: new Map() };
  const buffer = fs.readFileSync(aofPath);
  let offset = 0;

  isReplayingAof = true;
  while (offset < buffer.length) {
    const parsed = parseRESP(buffer, offset);
    if (!parsed.complete) {
      return;
    }

    handleCommand(parsed.value, transactionState, null);
    offset = parsed.nextOffset;
  }
  isReplayingAof = false;
}

function scheduleBLPOPTimeout(connection, listKey, timeoutSeconds) {
  if (timeoutSeconds <= 0) {
    return null;
  }

  return setTimeout(() => {
    const waiting = blockedClients.get(listKey);
    if (!waiting || waiting.length === 0) {
      return;
    }

    const index = waiting.findIndex((entry) => entry.connection === connection);
    if (index === -1) {
      return;
    }

    const [removedEntry] = waiting.splice(index, 1);
    if (waiting.length === 0) {
      blockedClients.delete(listKey);
    } else {
      blockedClients.set(listKey, waiting);
    }

    if (removedEntry) {
      connection.write(serializeNullArray());
    }
  }, timeoutSeconds * 1000);
}

function readLine(buffer, offset) {
  const end = buffer.indexOf("\r\n", offset);
  if (end === -1) {
    return { complete: false };
  }

  return {
    complete: true,
    line: buffer.toString("latin1", offset, end),
    nextOffset: end + 2,
  };
}

function parseRESP(buffer, offset = 0) {
  if (offset >= buffer.length) {
    return { complete: false };
  }

  const type = buffer[offset];

  if (type === 43) {
    const line = readLine(buffer, offset + 1);
    if (!line.complete) {
      return { complete: false };
    }

    return {
      complete: true,
      value: line.line,
      nextOffset: line.nextOffset,
    };
  }

  if (type === 45) {
    const line = readLine(buffer, offset + 1);
    if (!line.complete) {
      return { complete: false };
    }

    return {
      complete: true,
      value: line.line,
      nextOffset: line.nextOffset,
    };
  }

  if (type === 58) {
    const line = readLine(buffer, offset + 1);
    if (!line.complete) {
      return { complete: false };
    }

    return {
      complete: true,
      value: Number(line.line),
      nextOffset: line.nextOffset,
    };
  }

  if (type === 36) {
    const lengthLine = readLine(buffer, offset + 1);
    if (!lengthLine.complete) {
      return { complete: false };
    }

    const length = Number(lengthLine.line);
    if (length === -1) {
      return {
        complete: true,
        value: null,
        nextOffset: lengthLine.nextOffset,
      };
    }

    const totalLength = lengthLine.nextOffset + length + 2;
    if (totalLength > buffer.length) {
      return { complete: false };
    }

    const value = buffer.toString("utf8", lengthLine.nextOffset, lengthLine.nextOffset + length);
    return {
      complete: true,
      value,
      nextOffset: totalLength,
    };
  }

  if (type === 42) {
    const lengthLine = readLine(buffer, offset + 1);
    if (!lengthLine.complete) {
      return { complete: false };
    }

    const count = Number(lengthLine.line);
    const items = [];
    let cursor = lengthLine.nextOffset;

    for (let i = 0; i < count; i += 1) {
      const item = parseRESP(buffer, cursor);
      if (!item.complete) {
        return { complete: false };
      }
      items.push(item.value);
      cursor = item.nextOffset;
    }

    return {
      complete: true,
      value: items,
      nextOffset: cursor,
    };
  }

  return { complete: false };
}

function serializeBulkString(value) {
  const text = String(value ?? "");
  const length = Buffer.byteLength(text, "utf8");
  return `$${length}\r\n${text}\r\n`;
}

function serializeNullBulkString() {
  return "$-1\r\n";
}

function serializeInteger(value) {
  return `:${Number(value)}\r\n`;
}

function serializeArray(items) {
  const values = Array.isArray(items) ? items : [];
  const payload = values.map((item) => serializeBulkString(item)).join("");
  return `*${values.length}\r\n${payload}`;
}

function serializeRESPValue(value) {
  if (Array.isArray(value)) {
    const payload = value.map((item) => serializeRESPValue(item)).join("");
    return `*${value.length}\r\n${payload}`;
  }

  if (value === null) {
    return "$-1\r\n";
  }

  return serializeBulkString(value);
}

function serializeNullArray() {
  return "*-1\r\n";
}

function serializeError(message) {
  return `-ERR ${message}\r\n`;
}

function parseStreamEntryId(entryId) {
  if (typeof entryId !== "string") {
    return null;
  }

  const match = entryId.match(/^(\d+)-(\d+)$/);
  if (!match) {
    return null;
  }

  return {
    milliseconds: Number(match[1]),
    sequence: Number(match[2]),
  };
}

function isValidStreamEntryId(candidateId, lastEntryId) {
  const candidate = parseStreamEntryId(candidateId);
  if (!candidate) {
    return false;
  }

  if (candidate.milliseconds === 0 && candidate.sequence === 0) {
    return false;
  }

  if (!lastEntryId) {
    return candidate.milliseconds > 0 || candidate.sequence > 0;
  }

  const last = parseStreamEntryId(lastEntryId);
  if (!last) {
    return false;
  }

  if (candidate.milliseconds < last.milliseconds) {
    return false;
  }

  if (candidate.milliseconds === last.milliseconds) {
    return candidate.sequence > last.sequence;
  }

  return true;
}

function compareStreamEntryIds(leftId, rightId) {
  const left = parseStreamEntryId(leftId);
  const right = parseStreamEntryId(rightId);

  if (!left || !right) {
    return 0;
  }

  if (left.milliseconds < right.milliseconds) {
    return -1;
  }

  if (left.milliseconds > right.milliseconds) {
    return 1;
  }

  if (left.sequence < right.sequence) {
    return -1;
  }

  if (left.sequence > right.sequence) {
    return 1;
  }

  return 0;
}

function normalizeStreamRangeId(rawId, isEnd) {
  if (typeof rawId !== "string") {
    return null;
  }

  if (rawId === "-") {
    return {
      milliseconds: 0,
      sequence: 0,
    };
  }

  if (rawId === "+") {
    return {
      milliseconds: Number.MAX_SAFE_INTEGER,
      sequence: Number.MAX_SAFE_INTEGER,
    };
  }

  if (rawId.includes("-")) {
    return parseStreamEntryId(rawId);
  }

  const milliseconds = Number(rawId);
  if (Number.isNaN(milliseconds)) {
    return null;
  }

  return {
    milliseconds,
    sequence: isEnd ? Number.MAX_SAFE_INTEGER : 0,
  };
}

function pruneExpiredKeys() {
  const now = Date.now();
  for (const [key, entry] of store.entries()) {
    if (entry.expiresAt !== null && entry.expiresAt <= now) {
      store.delete(key);
    }
  }
}

function getStoredValue(key) {
  pruneExpiredKeys();

  const item = store.get(key);
  if (!item) {
    return undefined;
  }

  if (item.expiresAt !== null && item.expiresAt <= Date.now()) {
    store.delete(key);
    return undefined;
  }

  return item.value;
}

function resolveStreamStartId(streamKey, requestedId) {
  if (requestedId !== "$") {
    return requestedId;
  }

  const streamEntry = store.get(streamKey);
  if (!streamEntry || streamEntry.type !== "stream" || !Array.isArray(streamEntry.entries) || streamEntry.entries.length === 0) {
    return "0-0";
  }

  return streamEntry.entries[streamEntry.entries.length - 1].id;
}

function getEntriesAfterStreamId(streamKey, startId) {
  const streamEntry = store.get(streamKey);
  if (!streamEntry || streamEntry.type !== "stream" || !Array.isArray(streamEntry.entries) || streamEntry.entries.length === 0) {
    return [];
  }

  return streamEntry.entries.filter((entry) => compareStreamEntryIds(entry.id, startId) > 0);
}

function maybeWakeBlockedXReaders(streamKey) {
  if (blockedXReaders.length === 0) {
    return;
  }

  const validReaders = [];
  for (const waiting of blockedXReaders) {
    if (waiting.timeoutId) {
      clearTimeout(waiting.timeoutId);
    }

    const matches = [];
    for (let i = 0; i < waiting.streamKeys.length; i += 1) {
      const key = waiting.streamKeys[i];
      if (key !== streamKey) {
        continue;
      }

      const streamEntries = getEntriesAfterStreamId(key, waiting.ids[i]);
      if (streamEntries.length === 0) {
        continue;
      }

      const entryPayload = streamEntries.map((entry) => {
        const pairValues = [];
        for (const [field, value] of Object.entries(entry.fields)) {
          pairValues.push(field, value);
        }
        return [entry.id, pairValues];
      });

      matches.push([key, entryPayload]);
    }

    if (matches.length > 0) {
      waiting.connection.write(serializeRESPValue(matches));
      continue;
    }

    if (waiting.timeoutMs > 0) {
      waiting.timeoutId = setTimeout(() => {
        const index = blockedXReaders.indexOf(waiting);
        if (index !== -1) {
          blockedXReaders.splice(index, 1);
        }
        waiting.connection.write(serializeNullArray());
      }, waiting.timeoutMs);
    }

    validReaders.push(waiting);
  }

  blockedXReaders.length = 0;
  for (const waiting of validReaders) {
    blockedXReaders.push(waiting);
  }
}

function handleCommand(commandArray, transactionState, connection) {
  if (!Array.isArray(commandArray) || commandArray.length === 0) {
    return null;
  }

  const commandName = String(commandArray[0]).toUpperCase();

  const isSubscribed = transactionState?.subscriptions?.size > 0;
  const allowedInSubscribedMode = ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"];

  if (isSubscribed && !allowedInSubscribedMode.includes(commandName)) {
    return serializeError(`Can't execute '${String(commandArray[0]).toLowerCase()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`);
  }

  if (commandName === "WATCH") {
    if (transactionState.active) {
      return serializeError("WATCH inside MULTI is not allowed");
    }

    for (const key of commandArray.slice(1)) {
      const watchedKey = String(key);
      transactionState.watchedKeys.set(watchedKey, keyVersions.get(watchedKey) ?? 0);
    }
    return "+OK\r\n";
  }

  if (commandName === "UNWATCH") {
    transactionState.watchedKeys.clear();
    return "+OK\r\n";
  }

  if (transactionState.active && commandName !== "MULTI" && commandName !== "EXEC" && commandName !== "DISCARD") {
    transactionState.commands.push(commandArray);
    return "+QUEUED\r\n";
  }

  if (commandName === "PING") {
    if (transactionState?.subscriptions?.size > 0) {
      return serializeArray(["pong", ""]);
    }
    return "+PONG\r\n";
  }

  if (commandName === "SUBSCRIBE") {
    const channel = String(commandArray[1] ?? "");
    if (transactionState.subscriptions) {
      transactionState.subscriptions.add(channel);
    }
    
    if (connection) {
      if (!channelSubscribers.has(channel)) {
        channelSubscribers.set(channel, new Set());
      }
      channelSubscribers.get(channel).add(connection);
    }

    const subCount = transactionState.subscriptions ? transactionState.subscriptions.size : 0;
    return `*3\r\n${serializeBulkString("subscribe")}${serializeBulkString(channel)}${serializeInteger(subCount)}`;
  }

  if (commandName === "UNSUBSCRIBE") {
    const channel = String(commandArray[1] ?? "");
    if (transactionState.subscriptions && transactionState.subscriptions.has(channel)) {
      transactionState.subscriptions.delete(channel);
      if (channelSubscribers.has(channel)) {
        channelSubscribers.get(channel).delete(connection);
        if (channelSubscribers.get(channel).size === 0) {
          channelSubscribers.delete(channel);
        }
      }
    }
    const subCount = transactionState.subscriptions ? transactionState.subscriptions.size : 0;
    return `*3\r\n${serializeBulkString("unsubscribe")}${serializeBulkString(channel)}${serializeInteger(subCount)}`;
  }

  if (commandName === "PUBLISH") {
    const channel = String(commandArray[1] ?? "");
    const message = String(commandArray[2] ?? "");
    const subscribers = channelSubscribers.get(channel);
    
    if (subscribers) {
      const payload = serializeArray(["message", channel, message]);
      for (const conn of subscribers) {
        conn.write(payload);
      }
    }
    
    return serializeInteger(subscribers ? subscribers.size : 0);
  }

  if (commandName === "ZADD") {
    const key = String(commandArray[1] ?? "");
    const score = parseFloat(String(commandArray[2] ?? ""));
    const member = String(commandArray[3] ?? "");

    let zset = store.get(key);
    if (!zset || zset.type !== "zset") {
      zset = { type: "zset", members: new Map(), entries: [] };
      store.set(key, zset);
    }

    if (zset.members.has(member)) {
      // Always update: remove old entry, update score, re-insert in sorted order
      const oldIndex = zset.entries.findIndex((e) => e.member === member);
      if (oldIndex !== -1) {
        zset.entries.splice(oldIndex, 1);
      }
      zset.members.set(member, score);
      const updatedEntry = { member, score };
      let inserted = false;
      for (let i = 0; i < zset.entries.length; i++) {
        if (score < zset.entries[i].score || (score === zset.entries[i].score && member < zset.entries[i].member)) {
          zset.entries.splice(i, 0, updatedEntry);
          inserted = true;
          break;
        }
      }
      if (!inserted) {
        zset.entries.push(updatedEntry);
      }
      markKeyModified(key);
      return serializeInteger(0);
    }

    zset.members.set(member, score);
    const newEntry = { member, score };
    let inserted = false;
    for (let i = 0; i < zset.entries.length; i++) {
      if (score < zset.entries[i].score || (score === zset.entries[i].score && member < zset.entries[i].member)) {
        zset.entries.splice(i, 0, newEntry);
        inserted = true;
        break;
      }
    }
    if (!inserted) {
      zset.entries.push(newEntry);
    }

    markKeyModified(key);
    return serializeInteger(1);
  }

  if (commandName === "ZRANK") {
    const key = String(commandArray[1] ?? "");
    const member = String(commandArray[2] ?? "");

    const zset = store.get(key);
    if (!zset || zset.type !== "zset" || !zset.members.has(member)) {
      return serializeNullBulkString();
    }

    const index = zset.entries.findIndex((entry) => entry.member === member);
    return serializeInteger(index);
  }

  if (commandName === "ZRANGE") {
    const key = String(commandArray[1] ?? "");
    let start = parseInt(String(commandArray[2] ?? ""), 10);
    let end = parseInt(String(commandArray[3] ?? ""), 10);

    const zset = store.get(key);
    if (!zset || zset.type !== "zset") {
      return serializeArray([]);
    }

    const cardinality = zset.entries.length;

    // Normalize negative indexes according to the prompt's rules
    if (start < 0) {
      start = Math.max(0, cardinality + start);
    }
    if (end < 0) {
      end = Math.max(0, cardinality + end);
    }

    if (start >= cardinality || start > end) {
      return serializeArray([]);
    }

    const actualEnd = Math.min(end, cardinality - 1);
    const members = zset.entries.slice(start, actualEnd + 1).map((entry) => entry.member);
    return serializeArray(members);
  }

  if (commandName === "ZCARD") {
    const key = String(commandArray[1] ?? "");
    const zset = store.get(key);
    if (!zset || zset.type !== "zset") {
      return serializeInteger(0);
    }
    return serializeInteger(zset.entries.length);
  }

  if (commandName === "ZSCORE") {
    const key = String(commandArray[1] ?? "");
    const member = String(commandArray[2] ?? "");

    const zset = store.get(key);
    if (!zset || zset.type !== "zset" || !zset.members.has(member)) {
      return serializeNullBulkString();
    }

    return serializeBulkString(zset.members.get(member));
  }

  if (commandName === "ZREM") {
    const key = String(commandArray[1] ?? "");
    const member = String(commandArray[2] ?? "");

    const zset = store.get(key);
    if (!zset || zset.type !== "zset" || !zset.members.has(member)) {
      return serializeInteger(0);
    }

    zset.members.delete(member);
    const index = zset.entries.findIndex((e) => e.member === member);
    if (index !== -1) {
      zset.entries.splice(index, 1);
    }

    markKeyModified(key);
    return serializeInteger(1);
  }

  if (commandName === "GEOADD") {
    const key = String(commandArray[1] ?? "");
    const longitude = parseFloat(String(commandArray[2] ?? ""));
    const latitude = parseFloat(String(commandArray[3] ?? ""));
    const member = String(commandArray[4] ?? "");

    if (longitude < -180 || longitude > 180 || latitude < -85.05112878 || latitude > 85.05112878) {
      return serializeError(`invalid longitude,latitude pair ${longitude},${latitude}`);
    }

    let zset = store.get(key);
    if (!zset || zset.type !== "zset") {
      zset = { type: "zset", members: new Map(), entries: [] };
      store.set(key, zset);
    }

    const score = encodeGeoCoordinate(longitude, latitude);
    if (zset.members.has(member)) {
      const oldScore = zset.members.get(member);
      if (oldScore !== score) {
        const oldIndex = zset.entries.findIndex((e) => e.member === member);
        if (oldIndex !== -1) {
          zset.entries.splice(oldIndex, 1);
        }
        zset.members.set(member, score);
        const newEntry = { member, score };
        let inserted = false;
        for (let i = 0; i < zset.entries.length; i++) {
          if (score < zset.entries[i].score || (score === zset.entries[i].score && member < zset.entries[i].member)) {
            zset.entries.splice(i, 0, newEntry);
            inserted = true;
            break;
          }
        }
        if (!inserted) {
          zset.entries.push(newEntry);
        }
        markKeyModified(key);
      }
      return serializeInteger(0);
    }

    zset.members.set(member, score);
    const newEntry = { member, score };
    let inserted = false;
    for (let i = 0; i < zset.entries.length; i++) {
      if (score < zset.entries[i].score || (score === zset.entries[i].score && member < zset.entries[i].member)) {
        zset.entries.splice(i, 0, newEntry);
        inserted = true;
        break;
      }
    }
    if (!inserted) {
      zset.entries.push(newEntry);
    }

    markKeyModified(key);
    return serializeInteger(1);
  }

  if (commandName === "GEOPOS") {
    const key = String(commandArray[1] ?? "");
    const members = commandArray.slice(2);

    const zset = store.get(key);
    let response = `*${members.length}\r\n`;

    for (const member of members) {
      if (!zset || zset.type !== "zset" || !zset.members.has(String(member))) {
        response += "*-1\r\n";
      } else {
        const score = zset.members.get(String(member));
        const { longitude, latitude } = decodeGeoCoordinate(score);
        const lonStr = longitude.toFixed(17);
        const latStr = latitude.toFixed(17);
        const lonLen = lonStr.length;
        const latLen = latStr.length;
        response += `*2\r\n$${lonLen}\r\n${lonStr}\r\n$${latLen}\r\n${latStr}\r\n`;
      }
    }

    return response;
  }

  if (commandName === "GEODIST") {
    const key = String(commandArray[1] ?? "");
    const member1 = String(commandArray[2] ?? "");
    const member2 = String(commandArray[3] ?? "");

    const zset = store.get(key);
    if (!zset || zset.type !== "zset" || !zset.members.has(member1) || !zset.members.has(member2)) {
      return "$-1\r\n";
    }

    const coord1 = decodeGeoCoordinate(zset.members.get(member1));
    const coord2 = decodeGeoCoordinate(zset.members.get(member2));
    const distance = haversine(coord1.latitude, coord1.longitude, coord2.latitude, coord2.longitude);
    const distStr = distance.toFixed(4);
    return `$${distStr.length}\r\n${distStr}\r\n`;
  }

  if (commandName === "GEOSEARCH") {
    const key = String(commandArray[1] ?? "");
    const lon = parseFloat(String(commandArray[3] ?? ""));
    const lat = parseFloat(String(commandArray[4] ?? ""));
    const radius = parseFloat(String(commandArray[6] ?? ""));
    const unit = String(commandArray[7] ?? "m");

    const unitToMeters = { m: 1, km: 1000, mi: 1609.344, ft: 0.3048 };
    const radiusInMeters = radius * (unitToMeters[unit] ?? 1);

    const zset = store.get(key);
    if (!zset || zset.type !== "zset") {
      return "*0\r\n";
    }

    const results = [];
    for (const [member, score] of zset.members) {
      const coord = decodeGeoCoordinate(score);
      const distance = haversine(lat, lon, coord.latitude, coord.longitude);
      if (distance <= radiusInMeters) {
        results.push(member);
      }
    }

    return serializeRESPValue(results);
  }

  if (commandName === "CONFIG" && String(commandArray[1] ?? "").toUpperCase() === "GET") {
    const parameter = String(commandArray[2] ?? "").toLowerCase();
    if (Object.hasOwn(configuration, parameter)) {
      return serializeRESPValue([parameter, configuration[parameter]]);
    }

    return serializeRESPValue([]);
  }

  if (commandName === "REPLCONF") {
    const option = String(commandArray[1] ?? "").toUpperCase();
    if (option === "ACK") {
      connection.replicationOffset = Number(commandArray[2]) || 0;
      resolveAcknowledgedWaits();
      return null;
    }

    return "+OK\r\n";
  }

  if (commandName === "PSYNC") {
    connection.replicationOffset = 0;
    replicaConnections.add(connection);
    const fullResync = Buffer.from(`+FULLRESYNC ${masterReplicationId} ${masterReplicationOffset}\r\n`);
    const rdbHeader = Buffer.from(`$${emptyRdbFile.length}\r\n`);
    return Buffer.concat([fullResync, rdbHeader, emptyRdbFile]);
  }

  if (commandName === "WAIT") {
    const requiredReplicas = Number(commandArray[1]) || 0;
    const timeoutMs = Math.max(0, Number(commandArray[2]) || 0);
    const targetOffset = masterReplicationOffset;

    if (targetOffset === 0 || countAcknowledgedReplicas(targetOffset) >= requiredReplicas) {
      return serializeInteger(countAcknowledgedReplicas(targetOffset));
    }

    const waiting = {
      connection,
      requiredReplicas,
      targetOffset,
      timeoutId: null,
    };
    waiting.timeoutId = setTimeout(() => completeWait(waiting), timeoutMs);
    pendingWaits.add(waiting);

    const getAckCommand = serializeRESPValue(["REPLCONF", "GETACK", "*"]);
    for (const replicaConnection of replicaConnections) {
      replicaConnection.write(getAckCommand);
    }

    return null;
  }

  if (commandName === "INFO") {
    const section = String(commandArray[1] ?? "").toLowerCase();
    if (section === "replication") {
      return serializeBulkString([
        `role:${serverRole}`,
        `master_replid:${masterReplicationId}`,
        `master_repl_offset:${masterReplicationOffset}`,
      ].join("\r\n"));
    }

    return serializeBulkString("");
  }

  if (commandName === "MULTI") {
    transactionState.active = true;
    transactionState.commands = [];
    return "+OK\r\n";
  }

  if (commandName === "EXEC") {
    if (transactionState.active) {
      const queuedCommands = transactionState.commands;
      const watchedKeyChanged = [...transactionState.watchedKeys.entries()].some(
        ([key, version]) => (keyVersions.get(key) ?? 0) !== version,
      );
      transactionState.active = false;
      transactionState.commands = [];
      transactionState.watchedKeys.clear();

      if (watchedKeyChanged) {
        return serializeNullArray();
      }

      const responses = queuedCommands.map((queuedCommand) => (
        handleCommand.call(this, queuedCommand, transactionState)
      ));

      return `*${responses.length}\r\n${responses.join("")}`;
    }

    return serializeError("EXEC without MULTI");
  }

  if (commandName === "DISCARD") {
    if (!transactionState.active) {
      return serializeError("DISCARD without MULTI");
    }

    transactionState.active = false;
    transactionState.commands = [];
  transactionState.watchedKeys.clear();
    return "+OK\r\n";
  }

  if (commandName === "ECHO") {
    const argument = commandArray[1] ?? "";
    return serializeBulkString(argument);
  }

  if (commandName === "SET") {
    const key = commandArray[1];
    let value = commandArray[2];

    if (key === undefined || value === undefined) {
      return null;
    }

    value = String(value);
    let expiresAt = null;

    for (let i = 3; i < commandArray.length; i += 2) {
      const option = commandArray[i];
      const optionValue = commandArray[i + 1];

      if (option === undefined || optionValue === undefined) {
        break;
      }

      const normalizedOption = String(option).toUpperCase();
      if (normalizedOption === "PX") {
        const ms = Number(optionValue);
        if (!Number.isNaN(ms)) {
          expiresAt = Date.now() + ms;
        }
      }
    }

    const storeKey = String(key);
    store.set(storeKey, { value, expiresAt });
    markKeyModified(storeKey);
    const command = serializeRESPValue(commandArray);
    appendAofCommand(command);
    masterReplicationOffset += Buffer.byteLength(command);
    for (const replicaConnection of replicaConnections) {
      replicaConnection.write(command);
    }
    return "+OK\r\n";
  }

  if (commandName === "GET") {
    const key = commandArray[1];
    if (key === undefined) {
      return serializeNullBulkString();
    }

    const value = getStoredValue(String(key));
    if (value === undefined) {
      return serializeNullBulkString();
    }

    return serializeBulkString(value);
  }

  if (commandName === "KEYS") {
    if (String(commandArray[1] ?? "") !== "*") {
      return serializeRESPValue([]);
    }

    pruneExpiredKeys();
    return serializeRESPValue([...store.keys()]);
  }

  if (commandName === "INCR") {
    const key = commandArray[1];
    if (key === undefined) {
      return null;
    }

    const entry = store.get(String(key));
    if (!entry) {
      const storeKey = String(key);
      store.set(storeKey, { value: "1", expiresAt: null });
      markKeyModified(storeKey);
      return serializeInteger(1);
    }

    const value = Number(entry?.value);
    if (!Number.isInteger(value)) {
      return serializeError("value is not an integer or out of range");
    }

    const incrementedValue = value + 1;
  const storeKey = String(key);
  store.set(storeKey, { value: String(incrementedValue), expiresAt: entry.expiresAt });
  markKeyModified(storeKey);
    return serializeInteger(incrementedValue);
  }

  if (commandName === "TYPE") {
    const key = commandArray[1];
    if (key === undefined) {
      return "+none\r\n";
    }

    const entry = store.get(String(key));
    if (!entry) {
      return "+none\r\n";
    }

    if (entry.type === "stream") {
      return "+stream\r\n";
    }

    const value = getStoredValue(String(key));
    if (value === undefined) {
      return "+none\r\n";
    }

    if (Array.isArray(value)) {
      return "+list\r\n";
    }

    return "+string\r\n";
  }

  if (commandName === "XADD") {
    const key = commandArray[1];
    const entryId = commandArray[2];

    if (key === undefined || entryId === undefined || commandArray.length < 5) {
      return null;
    }

    const args = commandArray.slice(3);
    if (args.length % 2 !== 0) {
      return null;
    }

    const streamKey = String(key);
    const existing = store.get(streamKey);
    const stream = existing && existing.type === "stream" ? existing : { type: "stream", entries: [] };
    const lastEntry = stream.entries.length > 0 ? stream.entries[stream.entries.length - 1] : null;

    let resolvedEntryId = String(entryId);

    if (resolvedEntryId === "*") {
      const timePart = Date.now();
      const sequence = 0;
      resolvedEntryId = `${timePart}-${sequence}`;
    } else if (resolvedEntryId.endsWith("-*")) {
      const base = resolvedEntryId.slice(0, -2);
      const timePart = Number(base);
      if (Number.isNaN(timePart)) {
        return serializeError("The ID specified in XADD is equal or smaller than the target stream top item");
      }

      let nextSequence = 0;
      if (lastEntry) {
        const lastId = parseStreamEntryId(lastEntry.id);
        if (lastId && lastId.milliseconds === timePart) {
          nextSequence = lastId.sequence + 1;
        } else if (lastId && lastId.milliseconds > timePart) {
          return serializeError("The ID specified in XADD is equal or smaller than the target stream top item");
        }
      }

      if (timePart === 0 && lastEntry === null) {
        nextSequence = 1;
      }

      resolvedEntryId = `${timePart}-${nextSequence}`;
    }

    if (resolvedEntryId === "0-0") {
      return serializeError("The ID specified in XADD must be greater than 0-0");
    }

    if (!isValidStreamEntryId(resolvedEntryId, lastEntry ? lastEntry.id : null)) {
      return serializeError("The ID specified in XADD is equal or smaller than the target stream top item");
    }

    const fields = {};
    for (let i = 0; i < args.length; i += 2) {
      fields[String(args[i])] = String(args[i + 1]);
    }

    stream.entries.push({ id: resolvedEntryId, fields });
    store.set(streamKey, { type: "stream", entries: stream.entries, expiresAt: null });
    markKeyModified(streamKey);
    maybeWakeBlockedXReaders(streamKey);
    return serializeBulkString(resolvedEntryId);
  }

  if (commandName === "XREAD") {
    let cursor = 1;
    let timeoutMs = 0;

    if (String(commandArray[cursor] ?? "").toUpperCase() === "BLOCK") {
      cursor += 1;
      const timeoutValue = Number(commandArray[cursor]);
      if (!Number.isNaN(timeoutValue)) {
        timeoutMs = timeoutValue;
      }
      cursor += 1;
    }

    const streamsIndex = commandArray.findIndex((arg, index) => index >= cursor && String(arg).toUpperCase() === "STREAMS");
    if (streamsIndex === -1) {
      return serializeNullArray();
    }

    const streamArgs = commandArray.slice(streamsIndex + 1);
    if (streamArgs.length === 0 || streamArgs.length % 2 !== 0) {
      return serializeNullArray();
    }

    const streamCount = streamArgs.length / 2;
    const result = [];

    for (let i = 0; i < streamCount; i += 1) {
      const streamKey = String(streamArgs[i]);
      const startId = resolveStreamStartId(streamKey, String(streamArgs[streamCount + i]));
      const matches = getEntriesAfterStreamId(streamKey, startId);
      if (matches.length === 0) {
        continue;
      }

      const entryPayload = matches.map((entry) => {
        const pairValues = [];
        for (const [field, value] of Object.entries(entry.fields)) {
          pairValues.push(field, value);
        }
        return [entry.id, pairValues];
      });

      result.push([streamKey, entryPayload]);
    }

    if (result.length > 0) {
      return serializeRESPValue(result);
    }

    if (timeoutMs < 0) {
      return serializeNullArray();
    }

    const waiting = {
      connection: this,
      streamKeys: [],
      ids: [],
      timeoutMs,
      timeoutId: null,
    };

    for (let i = 0; i < streamCount; i += 1) {
      const streamKey = String(streamArgs[i]);
      const startId = resolveStreamStartId(streamKey, String(streamArgs[streamCount + i]));
      waiting.streamKeys.push(streamKey);
      waiting.ids.push(startId);
    }

    if (timeoutMs > 0) {
      waiting.timeoutId = setTimeout(() => {
        const index = blockedXReaders.indexOf(waiting);
        if (index !== -1) {
          blockedXReaders.splice(index, 1);
        }
        waiting.connection.write(serializeNullArray());
      }, timeoutMs);
    }

    blockedXReaders.push(waiting);
    return null;
  }

  if (commandName === "XRANGE") {
    const key = commandArray[1];
    const startRaw = commandArray[2];
    const endRaw = commandArray[3];

    if (key === undefined || startRaw === undefined || endRaw === undefined) {
      return serializeRESPValue([]);
    }

    const streamEntry = store.get(String(key));
    if (!streamEntry || streamEntry.type !== "stream" || !Array.isArray(streamEntry.entries) || streamEntry.entries.length === 0) {
      return serializeRESPValue([]);
    }

    const start = normalizeStreamRangeId(String(startRaw), false);
    const end = normalizeStreamRangeId(String(endRaw), true);

    if (!start || !end) {
      return serializeRESPValue([]);
    }

    const matches = streamEntry.entries.filter((entry) => {
      const entryId = parseStreamEntryId(entry.id);
      if (!entryId) {
        return false;
      }

      const startComparison = compareStreamEntryIds(entry.id, `${start.milliseconds}-${start.sequence}`);
      const endComparison = compareStreamEntryIds(entry.id, `${end.milliseconds}-${end.sequence}`);

      return startComparison >= 0 && endComparison <= 0;
    });

    const response = matches.map((entry) => {
      const pairValues = [];
      for (const [field, value] of Object.entries(entry.fields)) {
        pairValues.push(field, value);
      }
      return [entry.id, pairValues];
    });

    return serializeRESPValue(response);
  }

  if (commandName === "RPUSH") {
    const key = commandArray[1];

    if (key === undefined || commandArray.length < 3) {
      return null;
    }

    const listKey = String(key);
    const existing = store.get(listKey);
    const list = existing && Array.isArray(existing.value) ? existing.value.slice() : [];

    for (let i = 2; i < commandArray.length; i += 1) {
      list.push(String(commandArray[i]));
    }

    store.set(listKey, { value: list, expiresAt: existing?.expiresAt ?? null });
  markKeyModified(listKey);
    maybeWakeBlockedClient(listKey);
    return serializeInteger(list.length);
  }

  if (commandName === "LPUSH") {
    const key = commandArray[1];

    if (key === undefined || commandArray.length < 3) {
      return null;
    }

    const listKey = String(key);
    const existing = store.get(listKey);
    const list = existing && Array.isArray(existing.value) ? existing.value.slice() : [];

    const newItems = [];
    for (let i = commandArray.length - 1; i >= 2; i -= 1) {
      newItems.push(String(commandArray[i]));
    }

    const nextList = [...newItems, ...list];
    store.set(listKey, { value: nextList, expiresAt: existing?.expiresAt ?? null });
  markKeyModified(listKey);
    return serializeInteger(nextList.length);
  }

  if (commandName === "LLEN") {
    const key = commandArray[1];
    if (key === undefined) {
      return serializeInteger(0);
    }

    const item = getStoredValue(String(key));
    if (!item || !Array.isArray(item)) {
      return serializeInteger(0);
    }

    return serializeInteger(item.length);
  }

  if (commandName === "LPOP") {
    const key = commandArray[1];
    if (key === undefined) {
      return serializeNullBulkString();
    }

    const listKey = String(key);
    const item = getStoredValue(listKey);
    if (!item || !Array.isArray(item) || item.length === 0) {
      return serializeNullBulkString();
    }

    const count = commandArray[2] === undefined ? 1 : Number(commandArray[2]);
    if (Number.isNaN(count) || count <= 0) {
      return serializeNullBulkString();
    }

    const removedCount = Math.min(count, item.length);
    const removed = item.slice(0, removedCount);
    const remaining = item.slice(removedCount);
    store.set(listKey, { value: remaining, expiresAt: null });
    markKeyModified(listKey);

    if (removedCount === 1) {
      return serializeBulkString(removed[0]);
    }

    return serializeArray(removed);
  }

  if (commandName === "BLPOP") {
    const key = commandArray[1];
    const timeout = commandArray[2];

    if (key === undefined || timeout === undefined) {
      return serializeNullArray();
    }

    const listKey = String(key);
    const item = getStoredValue(listKey);
    if (Array.isArray(item) && item.length > 0) {
      const [removed] = item;
      const remaining = item.slice(1);
      store.set(listKey, { value: remaining, expiresAt: null });
      markKeyModified(listKey);
      return serializeArray([listKey, removed]);
    }

    const waiting = blockedClients.get(listKey) ?? [];
    waiting.push({ key: listKey, connection: this });
    blockedClients.set(listKey, waiting);

    if (Number(timeout) > 0) {
      scheduleBLPOPTimeout(this, listKey, Number(timeout));
    }

    return null;
  }

  if (commandName === "LRANGE") {
    const key = commandArray[1];
    const startRaw = Number(commandArray[2]);
    const stopRaw = Number(commandArray[3]);

    if (key === undefined || Number.isNaN(startRaw) || Number.isNaN(stopRaw)) {
      return serializeArray([]);
    }

    const item = getStoredValue(String(key));
    if (!item || !Array.isArray(item)) {
      return serializeArray([]);
    }

    let start = startRaw;
    let stop = stopRaw;

    if (start < 0) {
      start = Math.max(0, item.length + start);
    }

    if (stop < 0) {
      stop = Math.max(-1, item.length + stop);
    }

    if (start >= item.length || start > stop) {
      return serializeArray([]);
    }

    const normalizedStop = Math.min(stop, item.length - 1);
    const result = item.slice(start, normalizedStop + 1);
    return serializeArray(result);
  }

  return null;
}

const server = net.createServer((connection) => {
  let buffer = Buffer.alloc(0);
  const transactionState = { active: false, commands: [], watchedKeys: new Map(), subscriptions: new Set() };

  connection.on("close", () => {
    replicaConnections.delete(connection);
    for (const [channel, subscribers] of channelSubscribers) {
      if (subscribers.has(connection)) {
        subscribers.delete(connection);
        if (subscribers.size === 0) {
          channelSubscribers.delete(channel);
        }
      }
    }
  });

  connection.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (true) {
      const parsed = parseRESP(buffer, 0);
      if (!parsed.complete) {
        break;
      }

      const response = handleCommand.call(connection, parsed.value, transactionState, connection);
      if (response) {
        connection.write(response);
      }

      buffer = buffer.slice(parsed.nextOffset);
    }
  });
});

function maybeWakeBlockedClient(listKey) {
  const waiting = blockedClients.get(listKey);
  if (!waiting || waiting.length === 0) {
    return;
  }

  const next = waiting.shift();
  if (!next) {
    return;
  }

  const item = getStoredValue(listKey);
  if (!item || !Array.isArray(item) || item.length === 0) {
    return;
  }

  const [removed] = item;
  const remaining = item.slice(1);
  store.set(listKey, { value: remaining, expiresAt: null });
  markKeyModified(listKey);
  next.connection.write(serializeArray([listKey, removed]));

  if (waiting.length === 0) {
    blockedClients.delete(listKey);
  } else {
    blockedClients.set(listKey, waiting);
  }
}

const portIndex = process.argv.indexOf("--port");
const configuredPort = portIndex === -1 ? NaN : Number(process.argv[portIndex + 1]);
const port = Number.isInteger(configuredPort) ? configuredPort : 6379;

if (configuration.appendonly === "yes") {
  const appendOnlyDirectory = path.join(configuration.dir, configuration.appenddirname);
  const incrementalFilename = `${configuration.appendfilename}.1.incr.aof`;
  const manifestPath = path.join(appendOnlyDirectory, `${configuration.appendfilename}.manifest`);
  fs.mkdirSync(appendOnlyDirectory, { recursive: true });
  fs.closeSync(fs.openSync(path.join(appendOnlyDirectory, incrementalFilename), "a"));
  if (!fs.existsSync(manifestPath)) {
    fs.writeFileSync(manifestPath, `file ${incrementalFilename} seq 1 type i\n`);
  }
}

loadRdbFile();
replayAofFile();

server.listen(port, "127.0.0.1", () => {
  const masterPortNumber = Number(masterPort);
  if (serverRole !== "slave" || !masterHost || !Number.isInteger(masterPortNumber)) {
    return;
  }

  const masterConnection = net.createConnection({ host: masterHost, port: masterPortNumber });
  masterConnection.on("connect", () => {
    masterConnection.write("*1\r\n$4\r\nPING\r\n");
  });
  let handshakeStep = 0;
  let masterResponse = Buffer.alloc(0);
  let rdbLength = null;
  let replicationOffset = 0;
  const replicaTransactionState = { active: false, commands: [], watchedKeys: new Map() };
  masterConnection.on("data", (chunk) => {
    masterResponse = Buffer.concat([masterResponse, chunk]);

    while (true) {
      if (handshakeStep === 4) {
        if (rdbLength === null) {
          const headerEnd = masterResponse.indexOf("\r\n");
          if (headerEnd === -1) {
            break;
          }

          rdbLength = Number(masterResponse.toString("ascii", 1, headerEnd));
          masterResponse = masterResponse.slice(headerEnd + 2);
        }

        if (masterResponse.length < rdbLength) {
          break;
        }

        masterResponse = masterResponse.slice(rdbLength);
        rdbLength = null;
        handshakeStep = 5;
        continue;
      }

      const parsed = parseRESP(masterResponse, 0);
      if (!parsed.complete) {
        break;
      }

      const commandLength = parsed.nextOffset;
      masterResponse = masterResponse.slice(parsed.nextOffset);
      if (handshakeStep === 5) {
        const propagatedCommand = String(parsed.value[0] ?? "").toUpperCase();
        const propagatedOption = String(parsed.value[1] ?? "").toUpperCase();
        if (propagatedCommand === "REPLCONF" && propagatedOption === "GETACK") {
          masterConnection.write(serializeRESPValue(["REPLCONF", "ACK", String(replicationOffset)]));
        } else {
          handleCommand.call(masterConnection, parsed.value, replicaTransactionState, masterConnection);
        }
        replicationOffset += commandLength;
      } else if (handshakeStep === 0 && parsed.value === "PONG") {
        masterConnection.write(serializeRESPValue(["REPLCONF", "listening-port", String(port)]));
        handshakeStep = 1;
      } else if (handshakeStep === 1 && parsed.value === "OK") {
        masterConnection.write(serializeRESPValue(["REPLCONF", "capa", "psync2"]));
        handshakeStep = 2;
      } else if (handshakeStep === 2 && parsed.value === "OK") {
        masterConnection.write(serializeRESPValue(["PSYNC", "?", "-1"]));
        handshakeStep = 3;
      } else if (handshakeStep === 3 && String(parsed.value).startsWith("FULLRESYNC")) {
        handshakeStep = 4;
      }
    }
  });
  masterConnection.on("error", () => {});
});
