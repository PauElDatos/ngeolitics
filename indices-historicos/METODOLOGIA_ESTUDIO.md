# Estudio de mercados y cambios de hegemonía — metodología

Actualizado: 2026-08-16

## Pregunta

¿Qué ocurre con los activos financieros del antiguo hegemón cuando el centro de gravedad económico y monetario mundial se desplaza hacia otra potencia?

La web **no** interpreta la pérdida de hegemonía como una fecha única ni presupone que deba provocar un desplome bursátil. Se distinguen cuatro medidas: precio, retorno total, retorno total real y aportación anual de dividendos.

## 1. Capa histórica/contextual

- España 1492–1810: stock monetario español estimado, expresado en equivalente de plata (ICPSR 139761 / EMSM). Es un **proxy monetario**, no un índice bursátil.
- España 1900–2020: H-IBEX.
- Holanda 1602–1698: acción de la VOC. Es **una empresa**, no un índice agregado.
- Holanda 1723–1794: serie de mercado de Ámsterdam (Golez–Koudijs).
- Reino Unido 1801–2025: reconstrucción de Londres aportada en el proyecto.
- Estados Unidos 1871–2026: reconstrucción del mercado estadounidense; no se denomina “S&P 500 oficial desde 1871”.
- Shanghái histórico 1871–1940 y SSE Composite 1990–2026 se mantienen como tramos separados; no se rellena el hueco.

Cada tramo se normaliza a 100 en su primer año visible. **Las alturas entre países no son niveles comparables de riqueza, capitalización ni hegemonía.**

Convención visual:
- línea sólida: mercado/cartera bursátil agregada;
- línea a guiones: acción individual (VOC);
- línea a puntos: proxy no bursátil (España imperial).

## 2. Capa de comparabilidad estricta: UK, USA y China

### Reino Unido y Estados Unidos

Fuente: Jordà–Schularick–Taylor Macrohistory Database, R6 (`JSTdatasetR6.xlsx`).

Variables:
- `eq_capgain`: retorno anual por precio;
- `eq_div_rtn`: componente anual de dividendo;
- `eq_tr`: retorno total anual;
- `cpi`: IPC.

Identidad utilizada para el índice nominal de retorno total:

`TR_t = TR_(t-1) × (1 + eq_tr_t)`

Retorno total real anual:

`r_real,t = (1 + eq_tr_t) / (CPI_t / CPI_(t-1)) - 1`

Cobertura homogénea utilizada: **1871–2020**.

### China moderna

El Shanghai Stock Exchange anunció el SSE Composite Total Return Index en 2020. Su base es 21-07-2020 = 3320,89 y es el índice de retorno total correspondiente al SSE Composite de precio. Desde 29-07-2024 su código en tiempo real es `000888` (SSE TR / 上证收益).

Fuentes:
- https://english.sse.com.cn/markets/indices/indexnews/c/5221435.shtml
- https://english.sse.com.cn/markets/indices/indexnews/c/c_20240715_10759850.shtml

Retornos totales completos utilizados:
- 2021: +7,04%
- 2022: −12,83%
- 2023: −1,09%

Referencia pública de esos retornos: https://app-web.chnfund.com/YHHEediting/202407/t20240729_4380566.html

Para 2024 se utiliza una **reconstrucción derivada**, no un dato anual directo:
- el SSE Composite terminó 2024 con +12,67%;
- desde la base 21-07-2020 hasta 31-12-2024 el SSE Total Return acumuló 11,32 puntos porcentuales más que el índice de precio (dato Wind publicado en 2025);
- en el tramo parcial de 2020 el retorno total superó al precio en 0,34 pp;
- combinando esos datos con los retornos completos 2021–2023 se obtiene un retorno total 2024 aproximado de **+15,92%**.

Fuente del diferencial parcial de 2020 y evolución del exceso por dividendos:
https://stock.finance.sina.com.cn/stock/go.php/vReport_Show/kind/lastest/rptid/775692921900/index.phtml

Fuente del diferencial acumulado hasta 31-12-2024:
https://caifuhao.eastmoney.com/news/20250212185232279500910

CPI chino usado para deflactar:
- 2021: +0,9%
- 2022: +2,0%
- 2023: +0,2%
- 2024: +0,2%

Fuentes oficiales NBS:
- https://www.stats.gov.cn/english/PressRelease/202201/t20220117_1826409.html
- https://www.stats.gov.cn/english/PressRelease/202301/t20230113_1891633.html
- https://www.stats.gov.cn/sj/zxfb/202401/t20240117_1946624.html
- https://www.stats.gov.cn/english/PressRelease/202501/t20250117_1958330.html

La “aportación de dividendos” china se aproxima como `retorno total − retorno de precio`; no se etiqueta como dividend yield contable.

## 3. Estudio de evento UK → USA

Se usa **1944 / Bretton Woods** como marcador institucional (`t = 0`). No se afirma que la hegemonía británica terminara exactamente ese día: la transición libra→dólar fue gradual y las guerras mundiales fueron parte esencial del proceso.

Ventana: 1894–2020 (`t = −50 ... +76`). UK y USA se rebajan a 100 en 1944 usando exactamente las mismas series JST.

### Resultado 1944–2020

| Medida | Reino Unido | Estados Unidos |
|---|---:|---:|
| Multiplicador de precio | ×95,49 | ×244,84 |
| CAGR precio | 6,18% | 7,51% |
| Multiplicador retorno total nominal | ×2.468,80 | ×2.920,88 |
| CAGR retorno total nominal | 10,83% | 11,07% |
| Multiplicador retorno total real | ×75,07 | ×216,11 |
| CAGR retorno total real | 5,85% | 7,33% |
| Aportación media anual del dividendo, 1945–2020 | 4,69% | 3,59% |

Máximo drawdown a frecuencia anual después de 1944:
- UK precio: −69,37% (1972→1974)
- UK retorno total: −64,14% (1972→1974)
- UK retorno total real: −71,66% (1972→1974)
- USA precio: −42,92% (1972→1974)
- USA retorno total: −38,75% (2007→2008)
- USA retorno total real: −47,19% (1972→1974)

## Interpretación que sí soporta el estudio

Los datos **no** sostienen que perder la hegemonía destruya mecánicamente la bolsa del antiguo hegemón. Reino Unido siguió generando una enorme acumulación de riqueza financiera después de 1944.

La conclusión más defendible es más matizada: el antiguo hegemón puede seguir creciendo en términos absolutos mientras pierde rendimiento relativo frente al nuevo centro financiero. Además, incorporar dividendos reduce muchísimo la aparente brecha UK–USA que se observa con índices exclusivamente de precio; al descontar la inflación local, la ventaja estadounidense vuelve a ampliarse con claridad.

Por tanto, la hipótesis que la web permite explorar no es “EE. UU. perderá la hegemonía y su bolsa caerá”, sino: **¿la futura pérdida de primacía de EE. UU. se manifestará como destrucción absoluta de riqueza o, más probablemente, como un coste de oportunidad frente al nuevo centro financiero?**

## Límites

- No hay una bolsa española agregada comparable en 1492; la línea española imperial es contextual.
- La VOC no equivale a un índice nacional.
- Los retornos reales usan el IPC de cada país, no una moneda o deflactor común.
- La cobertura total/real de China es mucho más corta que UK/USA.
- 1944 es un marcador analítico, no una fecha causal única.
- Las reconstrucciones históricas están sujetas a cambios de composición, cobertura y metodología.
