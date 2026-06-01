import { describe, expect, it } from 'vitest';
import type { ProviderKeyConfig } from '@/types';
import { sortCodexConfigsByPriority } from './sort';

describe('sortCodexConfigsByPriority', () => {
  const configs: ProviderKeyConfig[] = [
    { apiKey: 'first', baseUrl: 'https://first.example.com/v1', priority: 3 },
    { apiKey: 'unset', baseUrl: 'https://unset.example.com/v1' },
    { apiKey: 'highest', baseUrl: 'https://highest.example.com/v1', priority: 10 },
    { apiKey: 'also-highest', baseUrl: 'https://also-highest.example.com/v1', priority: 10 },
    { apiKey: 'lowest', baseUrl: 'https://lowest.example.com/v1', priority: -1 },
  ];

  it('sorts priorities high to low by default and treats missing priority as 0', () => {
    expect(sortCodexConfigsByPriority(configs).map((item) => item.originalIndex)).toEqual([
      2, 3, 0, 1, 4,
    ]);
  });

  it('sorts priorities low to high when requested and treats missing priority as 0', () => {
    expect(sortCodexConfigsByPriority(configs, 'asc').map((item) => item.originalIndex)).toEqual([
      4, 1, 0, 2, 3,
    ]);
  });

  it('preserves the source list order for equal effective priorities', () => {
    const tiedConfigs: ProviderKeyConfig[] = [
      { apiKey: 'a', priority: 2 },
      { apiKey: 'b', priority: 2 },
      { apiKey: 'c' },
      { apiKey: 'd' },
    ];

    expect(sortCodexConfigsByPriority(tiedConfigs).map((item) => item.config.apiKey)).toEqual([
      'a',
      'b',
      'c',
      'd',
    ]);
  });
});
