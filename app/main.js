const net = require("net");

const store = new Map();

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

function handleCommand(commandArray) {
  if (!Array.isArray(commandArray) || commandArray.length === 0) {
    return null;
  }

  const commandName = String(commandArray[0]).toUpperCase();

  if (commandName === "PING") {
    return "+PONG\r\n";
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

    store.set(String(key), { value, expiresAt });
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

  return null;
}

const server = net.createServer((connection) => {
  let buffer = Buffer.alloc(0);

  connection.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (true) {
      const parsed = parseRESP(buffer, 0);
      if (!parsed.complete) {
        break;
      }

      const response = handleCommand(parsed.value);
      if (response) {
        connection.write(response);
      }

      buffer = buffer.slice(parsed.nextOffset);
    }
  });
});

server.listen(6379, "127.0.0.1");
