from tabulate import tabulate


def print_pretty(df, limit=20, title=None):

    if title:
        print(f"\n====== {title} ======")

    data = df.limit(limit).collect()

    headers = df.columns

    # Print table
    print(tabulate(data, headers=headers, tablefmt="psql"))
    print("\n")