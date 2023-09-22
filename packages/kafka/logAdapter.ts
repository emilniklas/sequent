import * as kafka from "kafkajs";
import * as sequent from "@sequent/core";

const oToK: Record<sequent.Log.Severity, kafka.logLevel> = {
  [sequent.Log.Severity.Fatal]: kafka.logLevel.ERROR,
  [sequent.Log.Severity.Error]: kafka.logLevel.ERROR,
  [sequent.Log.Severity.Warning]: kafka.logLevel.WARN,
  [sequent.Log.Severity.Info]: kafka.logLevel.INFO,
  [sequent.Log.Severity.Debug]: kafka.logLevel.DEBUG,
};

const kToO: Record<kafka.logLevel, sequent.Log.Severity | null> = {
  [kafka.logLevel.ERROR]: sequent.Log.Severity.Error,
  [kafka.logLevel.WARN]: sequent.Log.Severity.Warning,
  [kafka.logLevel.INFO]: sequent.Log.Severity.Info,
  [kafka.logLevel.DEBUG]: sequent.Log.Severity.Debug,
  [kafka.logLevel.NOTHING]: null,
};

export function logAdapter(logger: sequent.Logger): {
  logLevel: kafka.logLevel;
  logCreator: kafka.logCreator;
} {
  return {
    logLevel: logger.minSeverity
      ? oToK[logger.minSeverity]
      : kafka.logLevel.NOTHING,
    logCreator:
      () =>
      ({ log, level, namespace }) => {
        logger.logExact({
          severity: kToO[level] ?? sequent.Log.Severity.Debug,
          timestamp: new Date(log.timestamp),
          message: `${namespace}: ${log.message}`,
          context: {},
        });
      },
  };
}
