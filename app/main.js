const net = require("net");

const store = new Map();
const blockedClients = new Map();
const blockedXReaders = [];
const keyVersions = new Map();
const serverRole = process.argv.includes("--replicaof") ? "slave" : "master";

function markKeyModified(key) {
  const version = keyVersions.get(key) ?? 0;
  keyVersions.set(key, version + 1);
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

function handleCommand(commandArray, transactionState) {
  if (!Array.isArray(commandArray) || commandArray.length === 0) {
    return null;
  }

  const commandName = String(commandArray[0]).toUpperCase();

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
    return "+PONG\r\n";
  }

  if (commandName === "INFO") {
    const section = String(commandArray[1] ?? "").toLowerCase();
    if (section === "replication") {
      return serializeBulkString(`role:${serverRole}`);
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
  const transactionState = { active: false, commands: [], watchedKeys: new Map() };

  connection.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (true) {
      const parsed = parseRESP(buffer, 0);
      if (!parsed.complete) {
        break;
      }

      const response = handleCommand.call(connection, parsed.value, transactionState);
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

server.listen(port, "127.0.0.1");
