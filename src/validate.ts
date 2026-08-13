// Config validation shared by the helpers. Invalid numbers THROW: `NaN`
// disarms every comparison it touches, so coercing it into a default would
// hide a real caller bug behind quietly-different behavior.
import { TransportError } from './errors.js';

export function requirePositiveFinite(field: string, value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new TransportError(
      `invalid ${field}: ${value} (must be a positive finite number)`,
    );
  }
  return value;
}

export function requireNonNegativeFinite(field: string, value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new TransportError(
      `invalid ${field}: ${value} (must be a non-negative finite number)`,
    );
  }
  return value;
}

export function requireAtLeast(
  field: string,
  value: number,
  min: number,
): number {
  if (Number.isNaN(value) || value < min) {
    throw new TransportError(`invalid ${field}: ${value} (must be >= ${min})`);
  }
  return value;
}

export function requireRatio(field: string, value: number): number {
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new TransportError(`invalid ${field}: ${value} (must be in [0, 1])`);
  }
  return value;
}
