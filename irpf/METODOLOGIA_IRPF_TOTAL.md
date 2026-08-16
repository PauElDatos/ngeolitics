# Metodología — IRPF total, referencia Comunitat Valenciana (1990–2025)

## Objetivo

La vista **Esfuerzo fiscal** compara una estimación estatutaria del IRPF soportado por un contribuyente estándar a lo largo del tiempo. Para evitar la ruptura histórica causada por la cesión del IRPF a las comunidades autónomas, la vista por defecto usa una referencia territorial fija: **residente en la Comunitat Valenciana**.

- 1990–1996: escala nacional/unificada del proyecto.
- 1997–2025: **cuota estatal + cuota autonómica valenciana**.
- El selector **Solo estatal** conserva la serie del componente estatal para comparación.

No se pretende reconstruir una declaración real individual. El contribuyente estándar es: individual, menor de 65 años, sin hijos ni discapacidad, con la renta modelizada como rendimiento ordinario del trabajo.

## Fuentes aportadas

- `data_fuentes/raw_irpf.rtf`: HTML/texto copiado de BOE/AEAT por ejercicio.
- `data_fuentes/IRPF_tablas_1990-2023.json`: normalización semántica de esas tablas.
- `irpf_escala_estatal_1990-2025.xml`: serie estatal usada por la página.
- `irpf_autonomico_valencia_1997-2025.js`: serie autonómica valenciana curada para esta versión.

El JSON aportado se usa como **fuente de extracción y contraste**, no como verdad automática. El propio archivo advierte que no infiere comunidades ausentes ni corrige etiquetas duplicadas inventando una comunidad. Por eso las entradas manifiestamente desplazadas o duplicadas se verifican contra BOE/AEAT antes de entrar en la serie.

## Serie autonómica valenciana

| Periodo | Tratamiento |
|---|---|
| 1997 | Escala autonómica/complementaria de referencia del sistema de cesión. |
| 1998 | Ley valenciana 13/1997 remite a la escala autonómica prevista en la normativa estatal. |
| 1999–2001 | Escala valenciana 3 / 3,83 / 4,73 / 5,72 / 6,93 / 8,40, con actualización monetaria. |
| 2002 | Escala del nuevo sistema de financiación: 5,94 / 7,92 / 9,339 / 12,276 / 14,85 / 15,84. |
| 2003–2006 | 5,94 / 8,16 / 9,32 / 12,29 / 15,84. |
| 2007 | 7,94 / 9,43 / 12,66 / 15,77. |
| 2008 | Misma estructura, umbrales actualizados. |
| 2009 | 8,24 / 9,65 / 12,81 / 15,85. |
| 2010–2011 | 11,90 / 13,92 / 18,45 / 21,48, adaptada al nuevo porcentaje de cesión. |
| 2012–2013 | Escala temporal: 12 / 14 / 18,5 / 21,5 / 22,5 / 23,5. |
| 2014–2016 | 11,90 / 13,92 / 18,45 / 21,48 / 22,48 / 23,48. |
| 2017–2020 | 10 / 11 / 13,9 / 18 / 23,5 / 24,5 / 25 / 25,5. |
| 2021 | Añade 27,5 y 29,5 en la parte alta. |
| 2022 | Reforma autonómica con 11 tramos; primer tipo 9 %, máximo 29,5 %. |
| 2023–2025 | 11 tramos: 9 / 12 / 15 / 17,5 / 20 / 22,5 / 25 / 26,5 / 27,5 / 28,5 / 29,5. |

## Corrección especial del ejercicio 2010

La versión anterior del XML utilizaba para 2010 la escala estatal 15,66 / 18,27 / 24,14 / 27,13, perteneciente al esquema previo de reparto, y al añadir una escala autonómica ya adaptada al nuevo 50 % se habría producido una **doble contabilización conceptual**.

La Ley 22/2009 redactó la escala estatal como 12 / 14 / 18,5 / 21,5 y la Comunitat Valenciana aprobó 11,90 / 13,92 / 18,45 / 21,48. Esta versión corrige el XML estatal de 2010 y combina esas dos escalas dentro del mismo régimen de financiación.

## Mínimo personal

- Se conserva el mínimo estatal del modelo histórico.
- Para la **cuota autonómica valenciana**, desde el ejercicio 2022 se usa **6.105 €** para el contribuyente estándar, conforme a la regulación autonómica.
- Antes de 2022 se usa el mínimo general estatal como referencia autonómica del modelo.

## Cálculo del esfuerzo

Para cada muestra de renta dentro de cada grupo WID:

1. Se aplican los gastos/reducciones de rendimientos del trabajo modelizados.
2. Se calcula la cuota estatal progresiva.
3. Desde 1997 se calcula la cuota autonómica valenciana progresiva.
4. Desde 2007 se resta por separado la cuota que corresponde al mínimo personal con cada escala.
5. Se suman Estado + Comunitat Valenciana para el modo **Total · C. Valenciana**.

Luego:

`tipo efectivo = impuesto estimado / renta`

y para la aportación:

`aportación del grupo = impuesto agregado estimado del grupo / impuesto agregado estimado total`

La distribución interna de los grupos WID se aproxima con las fronteras percentiles y se calibra para reproducir la renta media de cada grupo.

## Límites

Esta es una **estimación estatutaria comparable**, no una estadística de cuotas efectivamente declaradas. No se dispone a nivel individual de:

- cotizaciones sociales;
- composición exacta entre renta del trabajo, capital y otras fuentes;
- estado civil, hijos, discapacidad o edad;
- todas las deducciones estatales y autonómicas;
- comportamiento fiscal, exenciones y circunstancias particulares.

Por tanto, la serie sirve para estudiar la **evolución de la carga legal comparable** bajo un perfil fijo, no para afirmar cuánto pagó realmente cada percentil en Hacienda.

## Fuentes legales principales

- BOE-A-1996-29118 — componente autonómico/complementario 1997.
- BOE-A-1998-8202 — Ley 13/1997 de la Comunitat Valenciana.
- BOE-A-1999-3255 — reforma de la escala valenciana desde 1999.
- BOE-A-2001-8543 — conversión oficial a euros.
- BOE-A-2001-24962 — sistema de financiación 2002.
- BOE-A-2004-4347 — TRLIRPF y escala complementaria del periodo.
- BOE-A-2007-9769 — escala valenciana 2007.
- BOE-A-2008-4062 — escala valenciana 2008.
- BOE-A-2009-20375 — nuevo sistema de financiación y escala estatal.
- BOE-A-2010-1279 — escala valenciana adaptada al nuevo reparto.
- BOE-A-2010-11417 — transición del IRPF 2010 para Comunitat Valenciana.
- DOGV-r-2012-90000 — escala valenciana temporal 2012–2013.
- Manuales AEAT 2014–2025 — contraste de escalas autonómicas y mínimos.
