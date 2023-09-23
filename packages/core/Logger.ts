import chalk, { ChalkInstance } from "chalk";
import { Serializer } from "./Codec.js";
import { JSONSerializer } from "./JSONCodec.js";
import { write } from "node:fs";

export class Logger<TFormat = any> {
  readonly #formatter: Log.Formatter<TFormat>;
  readonly #sink: Log.Sink<TFormat>;
  readonly #context: object;

  constructor(
    formatter: Log.Formatter<TFormat>,
    sink: Log.Sink<TFormat>,
    context?: object,
  ) {
    this.#formatter = formatter;
    this.#sink = sink;
    this.#context = context ?? {};

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

  withContext(context: object): Logger {
    const logger = new Logger(this.#formatter, this.#sink, {
      ...this.#context,
      ...context,
    });
    logger.#minSeverity = this.#minSeverity;
    return logger;
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
    if (Log.LEVELS[log.severity] <= this.#minSeverity) {
      this.#sink.log(
        this.#formatter.format({
          ...log,
          context: {
            ...this.#context,
            ...log.context,
          },
        }),
      );
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

  #logError(severity: Log.Severity, message: unknown, context?: object) {
    if (
      message &&
      typeof message === "object" &&
      "message" in message &&
      typeof message.message === "string" &&
      "stack" in message &&
      typeof message.stack === "string"
    ) {
      const errorContext = {
        ...context,
        stack: message.stack,
      };
      this.log(severity, message.message, errorContext);
    } else if (typeof message === "string") {
      this.log(severity, message, context);
    } else {
      this.log(severity, JSON.stringify(message), context);
    }
  }

  error(message: unknown, context?: object) {
    this.#logError(Log.Severity.Error, message, context);
  }

  fatal(message: unknown, context?: object) {
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
        this.#severityBadge(log.severity)(` ${log.severity} `),
        chalk.gray(log.timestamp.toLocaleTimeString()),
        this.#severityMessage(log.severity)(log.message),
        this.#formatContext(log.context).join(" "),
      ].join(" ");
    }

    #formatContext(context: unknown, path: (string | symbol)[] = []): string[] {
      if (context == null || typeof context !== "object") {
        const value =
          typeof context === "string" ? context : JSON.stringify(context);

        return [chalk.gray(chalk.underline(path.join(".")) + ": " + value)];
      }

      return Reflect.ownKeys(context).flatMap((key) =>
        this.#formatContext(context[key as keyof typeof context], [
          ...path,
          key,
        ]),
      );
    }

    #severityBadge(severity: Log.Severity): ChalkInstance {
      switch (severity) {
        case Severity.Debug:
          return chalk.bgGray.white;

        case Severity.Fatal:
          return chalk.bgRed.redBright;

        case Severity.Error:
          return chalk.bgRed.white;

        case Severity.Warning:
          return chalk.bgYellowBright.black;

        case Severity.Info:
          return chalk.bgBlueBright.black;
      }
    }

    #severityMessage(severity: Log.Severity): ChalkInstance {
      switch (severity) {
        case Severity.Debug:
          return chalk.gray;

        case Severity.Fatal:
          return chalk.red;

        case Severity.Error:
          return chalk.redBright;

        case Severity.Warning:
          return chalk.yellowBright;

        case Severity.Info:
          return chalk.blueBright;
      }
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
