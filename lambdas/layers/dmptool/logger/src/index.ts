import pino, { Logger } from 'pino';
import { pinoLambdaDestination } from 'pino-lambda';
import { ecsFormat } from '@elastic/ecs-pino-format';

// The available log levels
export enum LogLevel {
  trace = 'trace',
  debug = 'debug',
  info = 'info',
  warn = 'warn',
  error = 'error',
  fatal = 'fatal',
}

// Initialize a Lambda Logger
export const initializeLogger = (lambdaName: string, logLevel: LogLevel): Logger => {
  const destination = pinoLambdaDestination();
  const logger = pino(
    {
      // Set the minimum log level
      level: logLevel || 'info',
      // Format the log for OpenSearch using Elastic Common Schema
      ...ecsFormat
    },
    destination
  );

  // Define a standardized module name
  logger.child({ module: lambdaName });
  return logger;
}
