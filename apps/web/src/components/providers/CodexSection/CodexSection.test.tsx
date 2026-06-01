import { act } from 'react';
import { create, type ReactTestInstance, type ReactTestRenderer } from 'react-test-renderer';
import { describe, expect, it, vi } from 'vitest';
import { Button } from '@/components/ui/Button';
import { ToggleSwitch } from '@/components/ui/ToggleSwitch';
import type { ProviderKeyConfig } from '@/types';
import { CodexSection } from './CodexSection';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
  }),
}));

const getRows = (renderer: ReactTestRenderer) =>
  renderer.root.findAll((node) => node.type === 'div' && node.props.className === 'item-row');

const getText = (node: ReactTestInstance): string =>
  node.children.map((child) => (typeof child === 'string' ? child : getText(child))).join('');

const clickButton = (button: ReactTestInstance) => {
  const onClick = button.props.onClick as (() => void) | undefined;
  if (!onClick) throw new Error('Button click handler not found');

  act(() => {
    onClick();
  });
};

const toggleSwitch = (toggle: ReactTestInstance, value: boolean) => {
  const onChange = toggle.props.onChange as ((value: boolean) => void) | undefined;
  if (!onChange) throw new Error('Toggle change handler not found');

  act(() => {
    onChange(value);
  });
};

describe('CodexSection', () => {
  it('keeps sorted row actions mapped to original config indexes', () => {
    const configs: ProviderKeyConfig[] = [
      { apiKey: 'low-key', baseUrl: 'https://low.example.com/v1', priority: 1 },
      { apiKey: 'high-key', baseUrl: 'https://high.example.com/v1', priority: 9 },
      { apiKey: 'unset-key', baseUrl: 'https://unset.example.com/v1' },
    ];
    const onEdit = vi.fn();
    const onDelete = vi.fn();
    const onToggle = vi.fn();
    let renderer!: ReactTestRenderer;

    act(() => {
      renderer = create(
        <CodexSection
          configs={configs}
          usageByProvider={new Map()}
          loading={false}
          disableControls={false}
          isSwitching={false}
          onAdd={() => {}}
          onEdit={onEdit}
          onDelete={onDelete}
          onToggle={onToggle}
        />
      );
    });

    const firstDescendingRow = getRows(renderer)[0];
    expect(getText(firstDescendingRow)).toContain('https://high.example.com/v1');

    const [editHighButton, deleteHighButton] = firstDescendingRow.findAllByType(Button);
    clickButton(editHighButton);
    clickButton(deleteHighButton);
    toggleSwitch(firstDescendingRow.findByType(ToggleSwitch), false);

    expect(onEdit).toHaveBeenLastCalledWith(1);
    expect(onDelete).toHaveBeenLastCalledWith(1);
    expect(onToggle).toHaveBeenLastCalledWith(1, false);

    const sortButton = renderer.root
      .findAllByType(Button)
      .find((button) => button.props['aria-label'] === 'ai_providers.sort_descending');
    if (!sortButton) throw new Error('Sort button not found');
    clickButton(sortButton);

    const firstAscendingRow = getRows(renderer)[0];
    expect(getText(firstAscendingRow)).toContain('https://unset.example.com/v1');

    const [editLowButton, deleteLowButton] = firstAscendingRow.findAllByType(Button);
    clickButton(editLowButton);
    clickButton(deleteLowButton);
    toggleSwitch(firstAscendingRow.findByType(ToggleSwitch), false);

    expect(onEdit).toHaveBeenLastCalledWith(2);
    expect(onDelete).toHaveBeenLastCalledWith(2);
    expect(onToggle).toHaveBeenLastCalledWith(2, false);
  });
});
