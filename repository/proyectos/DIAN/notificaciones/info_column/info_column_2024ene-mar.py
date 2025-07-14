
# ***********************************
# /home/anacleto/Escritorio/itrc/ITRC/CSV/2024ene-mar.csv
{
    "int": [1, 2, 4, 6, 7, 9, 10, 16, 19, 28, 30, 31, 33, 35, 37, 44],
    "float": [12],
    "date": [11, 17, 20, 21, 22, 45, 47],
    "datetime": [40],
    "nit": [13],  # NUEVO TIPO NIT (posición 13)
    "str": list(set(range(1, 49)) - set([
        1, 2, 4, 6, 7, 9, 10, 16, 19, 28, 30, 31, 33, 35, 37, 44,  # int
        12,  # float
        11, 17, 20, 21, 22, 45, 47,  # date
        40,  # datetime
        13  # nit
    ]))
}
