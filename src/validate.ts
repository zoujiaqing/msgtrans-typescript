// Config validation shared across the package. Invalid numbers THROW: `NaN`
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

/** Finite lower-bounded value — `Infinity` is NOT acceptable here. */
export function requireFiniteAtLeast(
  field: string,
  value: number,
  min: number,
): number {
  if (!Number.isFinite(value) || value < min) {
    throw new TransportError(
      `invalid ${field}: ${value} (must be a finite number >= ${min})`,
    );
  }
  return value;
}

/** Positive integer count (queue caps, retries-style knobs). */
export function requireCount(field: string, value: number): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new TransportError(
      `invalid ${field}: ${value} (must be an integer >= 1)`,
    );
  }
  return value;
}

/** Attempt counts: a positive INTEGER, or `Infinity` for "never give up".
 *  1.5 attempts is not a thing. */
export function requireAttempts(field: string, value: number): number {
  if (value === Infinity) return value;
  if (!Number.isInteger(value) || value < 1) {
    throw new TransportError(
      `invalid ${field}: ${value} (must be an integer >= 1, or Infinity)`,
    );
  }
  return value;
}

export function requireRatio(field: string, value: number): number {
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new TransportError(`invalid ${field}: ${value} (must be in [0, 1])`);
  }
  return value;
}
