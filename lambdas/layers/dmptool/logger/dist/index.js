"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.initializeLogger = exports.LogLevel = void 0;
const pino_1 = __importDefault(require("pino"));
const pino_lambda_1 = require("pino-lambda");
const ecs_pino_format_1 = require("@elastic/ecs-pino-format");
// The available log levels
var LogLevel;
(function (LogLevel) {
    LogLevel["trace"] = "trace";
    LogLevel["debug"] = "debug";
    LogLevel["info"] = "info";
    LogLevel["warn"] = "warn";
    LogLevel["error"] = "error";
    LogLevel["fatal"] = "fatal";
})(LogLevel || (exports.LogLevel = LogLevel = {}));
// Initialize a Lambda Logger
const initializeLogger = (lambdaName, logLevel) => {
    const destination = (0, pino_lambda_1.pinoLambdaDestination)();
    const logger = (0, pino_1.default)({
        // Set the minimum log level
        level: logLevel || 'info',
        // Format the log for OpenSearch using Elastic Common Schema
        ...ecs_pino_format_1.ecsFormat
    }, destination);
    // Define a standardized module name
    logger.child({ module: lambdaName });
    return logger;
};
exports.initializeLogger = initializeLogger;
