import { act, create, type ReactTestRenderer } from 'react-test-renderer';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { monitoringAnalyticsApi } from '@/services/api/usageService';
import { useMonitoringAnalytics } from './useMonitoringAnalytics';

vi.mock('@/hooks/useRequestMonitoringAvailability', () => ({
  useRequestMonitoringAvailability: () => ({
    checking: false,
    available: true,
    managerServiceAvailable: true,
    modelPricesAvailable: true,
    serviceBase: 'http://manager.local',
    reason: '',
  }),
}));

vi.mock('@/stores', () => ({
  useAuthStore: (selector: (state: { managementKey: string }) => unknown) =>
    selector({ managementKey: 'admin-key' }),
}));

vi.mock('@/services/api/usageService', () => ({
  monitoringAnalyticsApi: {
    getAnalytics: vi.fn(),
  },
}));

const getAnalyticsMock = vi.mocked(monitoringAnalyticsApi.getAnalytics);

describe('useMonitoringAnalytics', () => {
  let renderer: ReactTestRenderer | null = null;

  afterEach(() => {
    renderer?.unmount();
    renderer = null;
    getAnalyticsMock.mockReset();
  });

  it('does not supersede an in-flight refresh for the same data scope', async () => {
    const resolvers: Array<(value: { generated_at_ms: number; granularity: string }) => void> = [];
    getAnalyticsMock.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolvers.push(resolve);
        })
    );

    function Harness({ nowMs }: { nowMs: number }) {
      useMonitoringAnalytics({
        fromMs: 1,
        toMs: nowMs,
        nowMs,
        dataScopeKey: 'today',
        include: { summary: true },
        throttleMs: 0,
      });
      return null;
    }

    await act(async () => {
      renderer = create(<Harness nowMs={10_000} />);
    });

    expect(getAnalyticsMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      renderer?.update(<Harness nowMs={15_000} />);
      await Promise.resolve();
    });

    expect(getAnalyticsMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolvers[0]?.({ generated_at_ms: 1, granularity: 'hour' });
      await Promise.resolve();
    });

    await act(async () => {
      renderer?.update(<Harness nowMs={20_000} />);
      await Promise.resolve();
    });

    expect(getAnalyticsMock).toHaveBeenCalledTimes(2);
  });
});
