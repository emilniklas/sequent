import { isNativeError } from "node:util/types";
import { Serializer } from "./Codec.js";
import { JSONSerializer } from "./JSONCodec.js";
import { write } from "node:fs";
import { inspect } from "node:util";

export class Logger<TFormat = any> {
  readonly #formatter: Log.Formatter<TFormat>;
  readonly #sink: Log.Sink<TFormat>;

  constructor(formatter: Log.Formatter<TFormat>, sink: Log.Sink<TFormat>) {
    this.#formatter = formatter;
    this.#sink = sink;

    switch (process.env.LOG_LEVEL?.toLowerCase()) {
      case "none":
      case "no":
      case "false":
      case "0":
        this.minSeverity = null;
        break;

      case "d":
      case "deb":
      case "debug":
      case "5":
        this.minSeverity = Log.Severity.Debug;
        break;

      case "i":
      case "inf":
      case "info":
      case "4":
      case undefined:
      case "":
        this.minSeverity = Log.Severity.Info;
        break;

      case "w":
      case "warn":
      case "warning":
      case "3":
        this.minSeverity = Log.Severity.Warning;
        break;

      case "e":
      case "err":
      case "error":
      case "2":
        this.minSeverity = Log.Severity.Error;
        break;

      case "f":
      case "fat":
      case "fatal":
      case "1":
        this.minSeverity = Log.Severity.Fatal;
        break;

      default:
        this.minSeverity = Log.Severity.Debug;
        this.error("Invalid log level", { level: process.env.LOG_LEVEL });
        break;
    }
  }

  #minSeverity!: number;

  set minSeverity(value: Log.Severity | null | undefined) {
    this.#minSeverity = value == null ? 0 : Log.LEVELS[value];
  }

  get minSeverity(): Log.Severity | undefined {
    return Log.LEVELS[this.#minSeverity];
  }

  log(severity: Log.Severity, message: string, context: object = {}) {
    this.logExact({
      severity,
      timestamp: new Date(),
      message,
      context,
    });
  }

  logExact(log: Log) {
    if (Log.LEVELS[log.severity] >= this.#minSeverity) {
      this.#sink.log(this.#formatter.format(log));
    }
  }

  debug(message: string, context?: object) {
    this.log(Log.Severity.Debug, message, context);
  }

  info(message: string, context?: object) {
    this.log(Log.Severity.Info, message, context);
  }

  warn(message: string, context?: object) {
    this.log(Log.Severity.Warning, message, context);
  }

  #logError(severity: Log.Severity, message: string | Error, context?: object) {
    if (isNativeError(message)) {
      const errorContext = {
        ...context,
        stack: message.stack,
      };
      this.log(severity, message.message, errorContext);
    } else {
      this.log(severity, message, context);
    }
  }

  error(message: string | Error, context?: object) {
    this.#logError(Log.Severity.Error, message, context);
  }

  fatal(message: string | Error, context?: object) {
    this.#logError(Log.Severity.Fatal, message, context);
  }
}

export interface Log {
  readonly severity: Log.Severity;
  readonly timestamp: Date;
  readonly message: string;
  readonly context: object;
}

export namespace Log {
  export enum Severity {
    Fatal = "FATAL",
    Error = "ERROR",
    Warning = "WARNING",
    Info = "INFO",
    Debug = "DEBUG",
  }

  export const LEVELS: readonly [undefined, ...Log.Severity[]] &
    Record<Severity, number> = Object.assign(
    [undefined, ...Object.values(Severity)] as const,
    Object.fromEntries(
      Object.values(Severity).map((severity, i) => [severity, i + 1]),
    ) as Record<Severity, number>,
  );

  export interface Formatter<TFormat> {
    format(log: Log): TFormat;
  }

  export interface Sink<TFormat> {
    log(log: TFormat): void;
  }
}

export namespace Log.Formatter {
  export class Pretty implements Log.Formatter<string> {
    format(log: Log): string {
      return [
        log.severity,
        log.timestamp.toLocaleString(),
        log.message,
        inspect(log.context),
      ].join(" ");
    }
  }

  export class Serialization implements Log.Formatter<Uint8Array> {
    readonly #serializer: Serializer<Log>;

    constructor(serializer: Serializer<Log>) {
      this.#serializer = serializer;
    }

    format(log: Log): Uint8Array {
      return this.#serializer.serialize(log);
    }
  }

  export const DEFAULT = process.stdout.isTTY
    ? new Pretty()
    : new Serialization(new JSONSerializer());
}

export namespace Log.Sink {
  export interface FileLike<TFormat> extends Log.Sink<TFormat> {}

  export class File implements FileLike<string | Uint8Array> {
    readonly fd: number;
    readonly encoding: BufferEncoding;

    constructor(fd: number, encoding: BufferEncoding = "utf8") {
      this.fd = fd;
      this.encoding = encoding;
    }

    log(log: string | Uint8Array): void {
      if (typeof log === "string") {
        log = Buffer.from(log + "\n", this.encoding);
      } else {
        log = Buffer.concat([log, Buffer.from("\n")]);
      }
      write(this.fd, log, (e) => {
        if (e != null) {
          throw e;
        }
      });
    }
  }

  export const DEFAULT = new File(1);
}

export namespace Logger {
  export const DEFAULT = new Logger<string | Uint8Array>(
    Log.Formatter.DEFAULT,
    Log.Sink.DEFAULT,
  );
}
