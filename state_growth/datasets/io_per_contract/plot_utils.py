from __future__ import annotations


def plot_top_contracts(props, n: int = 10_000) -> None:
    import toolplot
    import matplotlib.pyplot as plt

    for column in [
        'prop_slot_writes',
        'prop_slot_creates',
        'prop_slot_updates',
        'prop_slot_deletes',
        # 'prop_unique_slot_writes',
        # 'prop_unique_slot_creates',
        # 'prop_unique_slot_updates',
        # 'prop_unique_slot_deletes',
    ]:
        plt.plot(
            props[column].sort(descending=True)[:n].cum_sum(),
            label=column,
        )
    plt.xscale('log')
    toolplot.format_yticks(toolstr_kwargs={'percentage': True})
    toolplot.format_xticks()
    toolplot.add_tick_grid()
    plt.xlabel('top n contracts')
    plt.legend(loc='lower right')
    plt.title('Proportion of IO operations in top contracts')
    plt.show()
