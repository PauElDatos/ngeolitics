# Reforma del IRPF

Página estática basada en la maquetación de «Brecha entre ricos y pobres». Muestra los resultados de la hoja **Reforma** del archivo `Modelo_IRPF_tres_reformas_neutrales.xlsx` para Actual, VOX y TIPO A, B y C.

En escritorio, los escenarios se eligen a la izquierda. El panel derecho contiene cuatro miniaturas seleccionables y la tabla; la gráfica elegida ocupa el centro. En móvil las miniaturas aparecen encima de la gráfica. Al cambiar de vista se conservan los filtros y el salario consultado, ajustado al rango de la nueva gráfica.

Las cuatro gráficas conservan las series y los límites de los ejes del Excel. Los valores se leen de sus cachés incrustadas en Reforma. La tabla conserva las 16 filas de salario y los tipos efectivos de A86:C102 y E86:G102. `data.js` conserva el hash del archivo y los rangos de procedencia. El libro original no se publica ni se modifica.

Para actualizar desde una nueva versión del archivo, desde la raíz del repositorio:

```sh
python3 reforma-irpf/scripts/build_data.py /ruta/al/Modelo_IRPF_tres_reformas_neutrales.xlsx reforma-irpf/data.js
```

El extractor utiliza solo la biblioteca estándar de Python. Comprueba las cabeceras, las cuatro gráficas, la integridad de sus cachés y la concordancia entre el tipo efectivo de las curvas y la tabla. No recalcula el modelo. Después de actualizar, revisar los valores y la presentación en escritorio y móvil.

Para la vista local, servir la raíz del repositorio con `python3 -m http.server` y abrir `/reforma-irpf/`. No requiere instalación ni compilación.
