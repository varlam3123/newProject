def comparator(table1, table2):
    if table1.count() == table2.count():
        print("equal")
        if table1.exceptAll(table2).count() == 0:
            print('Таблицы идентичны!')
        else:
            print('Данные в результирующем выводе не совпадают')
    else:
        print('Количество строк в результирующем выводе не совпадает.')