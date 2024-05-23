//
// Similar to sourcify-server's logger

import { createLogger, transports, format } from "winston";
import chalk from "chalk";
import {
  setLibSourcifyLogger,
  setLibSourcifyLoggerLevel,
} from "@ethereum-sourcify/lib-sourcify";

const LogLevels = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 5,
  silly: 6,
};

export const validLogLevels = Object.values(LogLevels);

if (
  process.env.NODE_LOG_LEVEL &&
  !validLogLevels.includes(process.env.NODE_LOG_LEVEL)
) {
  throw new Error(`Invalid log level: ${process.env.NODE_LOG_LEVEL}`);
}

const loggerInstance = createLogger({
  level:
    process.env.NODE_LOG_LEVEL ||
    (process.env.NODE_ENV === "production" ? "info" : "debug"),
});

// 2024-03-06T17:04:16.375Z [warn]: [vera:pull] Received notification in 'new_verified_contract' - veraVerifiedContractId=192
const rawlineFormat = format.printf(
  ({ level, message, timestamp, service, ...metadata }) => {
    let msg = `${timestamp} [${level}]: ${service ? service : ""} ${chalk.bold(
      message
    )}`;
    if (metadata && Object.keys(metadata).length > 0) {
      msg += " - ";
      const metadataMsg = Object.entries(metadata)
        .map(([key, value]) => {
          if (value instanceof Error) {
            // JSON.stringify will give a "{}" on Error objects becuase message and stack properties are non-enumberable.
            // Instead do it manually
            value = JSON.stringify(value, Object.getOwnPropertyNames(value));
          } else if (typeof value === "object") {
            try {
              value = JSON.stringify(value);
            } catch (e) {
              value = "SerializationError: Unable to serialize object";
            }
          }
          return `${key}=${value}`;
        })
        .join(" | ");
      msg += chalk.grey(metadataMsg);
    }
    return msg;
  }
);

const lineFormat = format.combine(
  format.timestamp(),
  format.colorize(),
  rawlineFormat
);

const jsonFormat = format.combine(format.timestamp(), format.json());

const consoleTransport = new transports.Console({
  // NODE_LOG_LEVEL is takes precedence, otherwise use "info" if in production, "debug" otherwise
  format: process.env.NODE_ENV === "production" ? jsonFormat : lineFormat,
});

loggerInstance.add(consoleTransport);
const veraPullLoggerInstance = loggerInstance.child({
  service:
    process.env.NODE_ENV === "production"
      ? "vera:pull"
      : chalk.blue("[vera:pull]"),
});

export default veraPullLoggerInstance;

export const logLevelStringToNumber = (level) => {
  switch (level) {
    case "error":
      return LogLevels.error;
    case "warn":
      return LogLevels.warn;
    case "info":
      return LogLevels.info;
    case "debug":
      return LogLevels.debug;
    case "silly":
      return LogLevels.silly;
    default:
      return LogLevels.info;
  }
};

// Function to change the log level dynamically
export function setLogLevel(level) {
  if (!validLogLevels.includes(level)) {
    throw new Error(
      `Invalid log level: ${level}. level can take: ${validLogLevels.join(
        ", "
      )}`
    );
  }
  console.warn(`Setting log level to: ${level}`);
  consoleTransport.level = level;
  process.env.NODE_LOG_LEVEL = level;
  // Also set lib-sourcify's logger level
  setLibSourcifyLoggerLevel(logLevelStringToNumber(level));
}

// here we override the standard LibSourcify's Logger with a custom one
setLibSourcifyLogger({
  logLevel: logLevelStringToNumber(veraPullLoggerInstance.level),
  setLevel(level) {
    this.logLevel = level;
  },
  log(level, msg, metadata) {
    const logObject = {
      service:
        process.env.NODE_ENV === "production"
          ? "LibSourcify"
          : chalk.cyan("[LibSourcify]"),
      message: msg,
      ...metadata,
    };
    if (level <= this.logLevel) {
      switch (level) {
        case 0:
          veraPullLoggerInstance.error(logObject);
          break;
        case 1:
          veraPullLoggerInstance.warn(logObject);
          break;
        case 2:
          veraPullLoggerInstance.info(logObject);
          break;
        // Use winston's log levels https://github.com/winstonjs/winston?tab=readme-ov-file#logging-levels
        // We don't use http (3) and verbose (4)
        case 5:
          veraPullLoggerInstance.debug(logObject);
          break;
        case 6:
          veraPullLoggerInstance.silly(logObject);
          break;
        default:
          veraPullLoggerInstance.info(logObject);
          break;
      }
    }
  },
});
