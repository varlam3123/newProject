def comparator(table1, table2):
    if table1.count() == table2.count() and table1.exceptAll(table2).count() == 0:
        return True
    return False