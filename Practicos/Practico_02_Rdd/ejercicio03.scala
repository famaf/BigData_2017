/*
Ejercicio 3
===========
Implementar la acción *count* solo con la acción *aggregate*.
Hacer algunas pruebas con el programa.
*/

val input = sc.parallelize(1 to 30)
val result = input.aggregate(0)(
                             (acc, value) => (acc + 1),
                             (acc1, acc2) => (acc1 + acc2))
