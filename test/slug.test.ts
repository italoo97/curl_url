import { describe, expect, it } from 'vitest';
import { generateSlug, isValidSlug, SLUG_LENGTH } from '../src/lib/slug.js';

describe('generateSlug', () => {
  it('has the configured length', () => {
    expect(generateSlug()).toHaveLength(SLUG_LENGTH);
  });

  it('never emits ambiguous characters', () => {
    const sample = Array.from({ length: 200 }, () => generateSlug()).join('');
    expect(sample).not.toMatch(/[lI10O]/);
  });

  it('does not repeat across many draws', () => {
    const draws = new Set(Array.from({ length: 1000 }, () => generateSlug()));
    expect(draws.size).toBe(1000);
  });
});

describe('isValidSlug', () => {
  it('accepts a generated slug', () => {
    expect(isValidSlug(generateSlug())).toBe(true);
  });

  it.each(['', 'ab', 'has space', 'has/slash', 'l1I0O'])('rejects %j', (candidate) => {
    expect(isValidSlug(candidate)).toBe(false);
  });
});
