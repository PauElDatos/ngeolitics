window.NGE_LONG_RUN_DATA = {
  "meta": {
    "title": "Comparación histórica de precios nominales: Londres y serie estadounidense aportada",
    "measure": "Índices de precios nominales, sin dividendos y sin ajuste por inflación",
    "aggregation": "Londres: observación anual del libro reconstruido. Estados Unidos: último cierre disponible de cada año del CSV.",
    "london_start": 1801,
    "london_end": 2025,
    "london_count": 225,
    "usa_start": 1871,
    "usa_end": 2026,
    "usa_count": 156,
    "common_start": 1871,
    "common_end": 2025,
    "usa_last_date": "2026-08-03",
    "source_files": [
      {
        "name": "reconstruccion_indice_londres_1801_2025.xlsx",
        "sha256": "2542e864cd6b461e3e9597f5284c10ee5b5ce4adda2cd28c01fa29cde4ee7263"
      },
      {
        "name": "SP500_historico_completo_1871_2026.csv",
        "sha256": "39af9b1835b366fac47f63f33104e82ad149f202e8ed138e12e2a42110811ec2"
      }
    ],
    "caveats": [
      "Cada serie se expresa en su moneda local y se vuelve a basar sólo para comparar trayectorias porcentuales.",
      "Ambas series excluyen dividendos e inflación; no representan riqueza real ni retorno total.",
      "Londres es una reconstrucción compuesta: no es un índice FTSE oficial continuo desde 1801.",
      "El CSV estadounidense no incluye metadatos bibliográficos que permitan atribuir institucionalmente todos sus tramos.",
      "El dato estadounidense de 2026 es parcial hasta 2026-08-03."
    ]
  },
  "sources": {
    "london_boe": {
      "name": "Bank of England — A millennium of macroeconomic data for the UK",
      "url": "https://www.bankofengland.co.uk/statistics/research-datasets"
    },
    "london_lseg": {
      "name": "FTSE Russell / LSEG — FTSE UK Index Series",
      "url": "https://www.lseg.com/en/ftse-russell/indices/uk"
    },
    "usa_file": {
      "name": "SP500_historico_completo_1871_2026.csv",
      "url": null,
      "note": "Archivo aportado; source_kind/source_id cambian de ASSET 1764 a INDICATOR 211 en 2025."
    }
  },
  "london": [
    {
      "year": 1801,
      "value": 1.10579959,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1802,
      "value": 1.21423344,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1803,
      "value": 1.03125127,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1804,
      "value": 1.00527235,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1805,
      "value": 1.09789292,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1806,
      "value": 1.1837364,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1807,
      "value": 1.23004673,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1808,
      "value": 1.26167326,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1809,
      "value": 1.3497758,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1810,
      "value": 1.35881195,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1811,
      "value": 1.27748655,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1812,
      "value": 1.16905269,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1813,
      "value": 1.10579959,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1814,
      "value": 1.16114605,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1815,
      "value": 1.07756161,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1816,
      "value": 0.97138677,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1817,
      "value": 1.04141698,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1818,
      "value": 1.26619136,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1819,
      "value": 1.25941425,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1820,
      "value": 1.20180875,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1821,
      "value": 1.25489617,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1822,
      "value": 1.35655292,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1823,
      "value": 1.43336025,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1824,
      "value": 1.71912868,
      "segment": "1801-1824",
      "source_name": "Mirowski (1981)",
      "observation": "Promedio anual",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1825,
      "value": 1.61634241,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1826,
      "value": 1.48682779,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1827,
      "value": 1.51963816,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1828,
      "value": 1.52136502,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1829,
      "value": 1.55072167,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1830,
      "value": 1.47646662,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1831,
      "value": 1.40393844,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1832,
      "value": 1.4954621,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1833,
      "value": 1.60425438,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1834,
      "value": 1.65087964,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1835,
      "value": 1.77175995,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1836,
      "value": 1.96862217,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1837,
      "value": 2.08086817,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1838,
      "value": 2.33471681,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1839,
      "value": 2.30363331,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1840,
      "value": 2.51085669,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1841,
      "value": 2.45905085,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1842,
      "value": 2.58856546,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1843,
      "value": 3.00301223,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1844,
      "value": 3.41055156,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1845,
      "value": 4.09438873,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1846,
      "value": 4.19972729,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1847,
      "value": 3.54006617,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1848,
      "value": 3.44163507,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1849,
      "value": 3.35874571,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1850,
      "value": 3.98559645,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1851,
      "value": 4.27916292,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1852,
      "value": 5.52595696,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1853,
      "value": 5.10805646,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1854,
      "value": 5.151228,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1855,
      "value": 5.36535883,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1856,
      "value": 6.11827047,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1857,
      "value": 5.9145008,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1858,
      "value": 6.64323638,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1859,
      "value": 7.01105789,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1860,
      "value": 7.47731051,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1861,
      "value": 7.30807807,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1862,
      "value": 7.98846152,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1863,
      "value": 8.73791944,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1864,
      "value": 9.54954436,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1865,
      "value": 9.8396571,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1866,
      "value": 9.49255793,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1867,
      "value": 9.18690344,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1868,
      "value": 9.69805446,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1869,
      "value": 10.44233179,
      "segment": "1825-1869",
      "source_name": "Acheson et al. (2009)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1870,
      "value": 11.28676708,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1871,
      "value": 13.54637785,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1872,
      "value": 13.99069905,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1873,
      "value": 14.26491675,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1874,
      "value": 14.09373775,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1875,
      "value": 14.58560919,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1876,
      "value": 15.04651444,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1877,
      "value": 15.74015876,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1878,
      "value": 15.18295714,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1879,
      "value": 17.16888793,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1880,
      "value": 18.92698206,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1881,
      "value": 21.38181163,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1882,
      "value": 21.02473538,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1883,
      "value": 20.48439968,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1884,
      "value": 20.51922316,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1885,
      "value": 20.37969244,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1886,
      "value": 20.13106019,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1887,
      "value": 20.03040489,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1888,
      "value": 22.06950011,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1889,
      "value": 23.47312032,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1890,
      "value": 23.73132464,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1891,
      "value": 23.03125056,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1892,
      "value": 22.46007555,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1893,
      "value": 21.69867899,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1894,
      "value": 23.25881401,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1895,
      "value": 25.24976849,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1896,
      "value": 27.39599881,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1897,
      "value": 28.48635956,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1898,
      "value": 28.93929268,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1899,
      "value": 29.35601849,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1900,
      "value": 31.23186807,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1901,
      "value": 31.77217939,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1902,
      "value": 32.61731936,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1903,
      "value": 32.02042242,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1904,
      "value": 34.3995398,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1905,
      "value": 36.84190713,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1906,
      "value": 39.16663147,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1907,
      "value": 37.45504967,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1908,
      "value": 39.1367814,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1909,
      "value": 42.11117679,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1910,
      "value": 42.78495562,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1911,
      "value": 42.52396739,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1912,
      "value": 43.41697071,
      "segment": "1870-1912",
      "source_name": "Grossman (1997)",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1913,
      "value": 42.20129553,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1914,
      "value": 42.78105508,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1915,
      "value": 32.22524811,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1916,
      "value": 35.87257978,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1917,
      "value": 38.87626469,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1918,
      "value": 43.51052141,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1919,
      "value": 45.99928891,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1920,
      "value": 32.48270681,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1921,
      "value": 33.3409025,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1922,
      "value": 39.20890134,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1923,
      "value": 38.642106,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1924,
      "value": 42.30960527,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1925,
      "value": 44.17669581,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1926,
      "value": 45.24360469,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1927,
      "value": 48.97778577,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1928,
      "value": 52.94535317,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1929,
      "value": 49.04446758,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1930,
      "value": 39.50896946,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1931,
      "value": 30.24019857,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1932,
      "value": 31.9405846,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1933,
      "value": 36.14153831,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1934,
      "value": 37.84192434,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1935,
      "value": 40.0424239,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1936,
      "value": 45.91042274,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1937,
      "value": 38.50874239,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1938,
      "value": 34.80790221,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1939,
      "value": 33.60762972,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1940,
      "value": 31.17374384,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1941,
      "value": 33.20753889,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1942,
      "value": 35.54140207,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1943,
      "value": 37.84192434,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1944,
      "value": 39.9757421,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1945,
      "value": 41.80949174,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1946,
      "value": 43.40985506,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1947,
      "value": 41.07599188,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1948,
      "value": 40.54253744,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1949,
      "value": 38.97551502,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1950,
      "value": 40.52385342,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1951,
      "value": 41.50380436,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1952,
      "value": 39.40234591,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1953,
      "value": 45.70672126,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1954,
      "value": 61.46765962,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1955,
      "value": 62.43756352,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1956,
      "value": 56.82020344,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1957,
      "value": 54.9208083,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1958,
      "value": 73.15451666,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1959,
      "value": 104.89874386,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1960,
      "value": 99.95587111,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1961,
      "value": 97.42951393,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1962,
      "value": 94.95,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1963,
      "value": 107.86,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1964,
      "value": 97.43,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1965,
      "value": 103.52,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1966,
      "value": 92.48,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1967,
      "value": 122.18,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1968,
      "value": 168.6,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1969,
      "value": 142.31,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1970,
      "value": 133.61,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1971,
      "value": 187.66,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1972,
      "value": 221.39,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1973,
      "value": 144.97,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1974,
      "value": 65.26,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1975,
      "value": 154.28,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1976,
      "value": 141.53,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1977,
      "value": 211.56,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1978,
      "value": 224.4,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1979,
      "value": 230.77,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1980,
      "value": 292.72,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1981,
      "value": 310.04,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1982,
      "value": 379.4,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1983,
      "value": 464.97,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1984,
      "value": 577.75,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1985,
      "value": 674.53,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1986,
      "value": 817.98,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1987,
      "value": 844.74,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1988,
      "value": 914.63,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1989,
      "value": 1175.26,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1990,
      "value": 1040.25,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1991,
      "value": 1156.9,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1992,
      "value": 1324.08,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1993,
      "value": 1628.88,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1994,
      "value": 1502.42,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1995,
      "value": 1783.3,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1996,
      "value": 1976.41,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1997,
      "value": 2388.17,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1998,
      "value": 2595.88,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 1999,
      "value": 3146.68,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2000,
      "value": 2989.69,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2001,
      "value": 2507.59,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2002,
      "value": 1900.53,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2003,
      "value": 2168.86757416,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2004,
      "value": 2372.41359707,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2005,
      "value": 2805.81025419,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2006,
      "value": 3189.64430984,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2007,
      "value": 3267.13232207,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2008,
      "value": 2128.79989437,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2009,
      "value": 2708.09315365,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2010,
      "value": 3039.81841851,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2011,
      "value": 2814.19026689,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2012,
      "value": 3099.07838305,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2013,
      "value": 3514.57982883,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2014,
      "value": 3511.3791335,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2015,
      "value": 3401.1794426,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2016,
      "value": 3779.29673019,
      "segment": "1913-2016",
      "source_name": "Empalme mensual del Banco de Inglaterra",
      "observation": "Fin de año",
      "source_id": "BOE_A31_M13"
    },
    {
      "year": 2017,
      "value": 4119.44381297,
      "segment": "2017-2025",
      "source_name": "FTSE All-Share oficial (retorno anual encadenado)",
      "observation": "Último día de negociación",
      "source_id": "LSE_DAILY_2025"
    },
    {
      "year": 2018,
      "value": 3585.94485401,
      "segment": "2017-2025",
      "source_name": "FTSE All-Share oficial (retorno anual encadenado)",
      "observation": "Último día de negociación",
      "source_id": "LSE_DAILY_2025"
    },
    {
      "year": 2019,
      "value": 4094.71425724,
      "segment": "2017-2025",
      "source_name": "FTSE All-Share oficial (retorno anual encadenado)",
      "observation": "Último día de negociación",
      "source_id": "LSE_DAILY_2025"
    },
    {
      "year": 2020,
      "value": 3584.55209218,
      "segment": "2017-2025",
      "source_name": "FTSE All-Share oficial (retorno anual encadenado)",
      "observation": "Último día de negociación",
      "source_id": "LSE_DAILY_2025"
    },
    {
      "year": 2021,
      "value": 4105.98521804,
      "segment": "2017-2025",
      "source_name": "FTSE All-Share oficial (retorno anual encadenado)",
      "observation": "Último día de negociación",
      "source_id": "LSE_DAILY_2025"
    },
    {
      "year": 2022,
      "value": 3976.30835764,
      "segment": "2017-2025",
      "source_name": "FTSE All-Share oficial (retorno anual encadenado)",
      "observation": "Último día de negociación",
      "source_id": "LSE_DAILY_2025"
    },
    {
      "year": 2023,
      "value": 4129.39008007,
      "segment": "2017-2025",
      "source_name": "FTSE All-Share oficial (retorno anual encadenado)",
      "observation": "Último día de negociación",
      "source_id": "LSE_DAILY_2025"
    },
    {
      "year": 2024,
      "value": 4359.4598922,
      "segment": "2017-2025",
      "source_name": "FTSE All-Share oficial (retorno anual encadenado)",
      "observation": "Último día de negociación",
      "source_id": "LSE_DAILY_2025"
    },
    {
      "year": 2025,
      "value": 5220.64182301,
      "segment": "2017-2025",
      "source_name": "FTSE All-Share oficial (retorno anual encadenado)",
      "observation": "Último día de negociación",
      "source_id": "LSE_DAILY_2025"
    }
  ],
  "usa": [
    {
      "year": 1871,
      "value": 4.74,
      "last_date": "1871-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1872,
      "value": 4.95,
      "last_date": "1872-11-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1873,
      "value": 4.42,
      "last_date": "1873-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1874,
      "value": 4.54,
      "last_date": "1874-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1875,
      "value": 4.37,
      "last_date": "1875-12-01",
      "observations": 10,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1876,
      "value": 3.58,
      "last_date": "1876-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1877,
      "value": 3.26,
      "last_date": "1877-11-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1878,
      "value": 3.47,
      "last_date": "1878-11-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1879,
      "value": 4.92,
      "last_date": "1879-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1880,
      "value": 5.84,
      "last_date": "1880-12-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1881,
      "value": 6.01,
      "last_date": "1881-12-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1882,
      "value": 5.84,
      "last_date": "1882-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1883,
      "value": 5.46,
      "last_date": "1883-11-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1884,
      "value": 4.34,
      "last_date": "1884-12-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1885,
      "value": 5.2,
      "last_date": "1885-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1886,
      "value": 5.64,
      "last_date": "1886-12-01",
      "observations": 10,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1887,
      "value": 5.27,
      "last_date": "1887-12-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1888,
      "value": 5.24,
      "last_date": "1888-11-01",
      "observations": 7,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1889,
      "value": 5.35,
      "last_date": "1889-11-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1890,
      "value": 4.6,
      "last_date": "1890-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1891,
      "value": 5.41,
      "last_date": "1891-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1892,
      "value": 5.51,
      "last_date": "1892-12-01",
      "observations": 10,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1893,
      "value": 4.41,
      "last_date": "1893-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1894,
      "value": 4.34,
      "last_date": "1894-11-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1895,
      "value": 4.59,
      "last_date": "1895-11-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1896,
      "value": 4.22,
      "last_date": "1896-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1897,
      "value": 4.75,
      "last_date": "1897-12-01",
      "observations": 10,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1898,
      "value": 5.65,
      "last_date": "1898-12-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1899,
      "value": 6.02,
      "last_date": "1899-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1900,
      "value": 6.48,
      "last_date": "1900-11-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1901,
      "value": 8.08,
      "last_date": "1901-11-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1902,
      "value": 8.05,
      "last_date": "1902-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1903,
      "value": 6.57,
      "last_date": "1903-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1904,
      "value": 8.25,
      "last_date": "1904-12-01",
      "observations": 10,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1905,
      "value": 9.54,
      "last_date": "1905-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1906,
      "value": 9.93,
      "last_date": "1906-11-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1907,
      "value": 6.25,
      "last_date": "1907-11-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1908,
      "value": 9.03,
      "last_date": "1908-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1909,
      "value": 10.3,
      "last_date": "1909-12-01",
      "observations": 10,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1910,
      "value": 9.05,
      "last_date": "1910-12-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1911,
      "value": 9.11,
      "last_date": "1911-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1912,
      "value": 9.73,
      "last_date": "1912-11-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1913,
      "value": 8.04,
      "last_date": "1913-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1914,
      "value": 7.35,
      "last_date": "1914-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1915,
      "value": 9.48,
      "last_date": "1915-12-01",
      "observations": 10,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1916,
      "value": 9.8,
      "last_date": "1916-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1917,
      "value": 7.04,
      "last_date": "1917-11-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1918,
      "value": 8.06,
      "last_date": "1918-11-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1919,
      "value": 8.92,
      "last_date": "1919-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1920,
      "value": 6.81,
      "last_date": "1920-12-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1921,
      "value": 7.31,
      "last_date": "1921-12-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1922,
      "value": 8.78,
      "last_date": "1922-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1923,
      "value": 8.27,
      "last_date": "1923-11-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1924,
      "value": 10.16,
      "last_date": "1924-12-01",
      "observations": 9,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1925,
      "value": 12.46,
      "last_date": "1925-12-01",
      "observations": 8,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1926,
      "value": 13.49,
      "last_date": "1926-12-01",
      "observations": 10,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1927,
      "value": 17.66,
      "last_date": "1927-12-30",
      "observations": 10,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1928,
      "value": 24.35,
      "last_date": "1928-12-31",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1929,
      "value": 21.450001,
      "last_date": "1929-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1930,
      "value": 15.34,
      "last_date": "1930-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1931,
      "value": 8.12,
      "last_date": "1931-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1932,
      "value": 6.92,
      "last_date": "1932-12-30",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1933,
      "value": 9.97,
      "last_date": "1933-12-29",
      "observations": 242,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1934,
      "value": 9.5,
      "last_date": "1934-12-31",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1935,
      "value": 13.43,
      "last_date": "1935-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1936,
      "value": 17.18,
      "last_date": "1936-12-31",
      "observations": 254,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1937,
      "value": 10.55,
      "last_date": "1937-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1938,
      "value": 13.14,
      "last_date": "1938-12-30",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1939,
      "value": 12.46,
      "last_date": "1939-12-29",
      "observations": 249,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1940,
      "value": 10.58,
      "last_date": "1940-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1941,
      "value": 8.69,
      "last_date": "1941-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1942,
      "value": 9.77,
      "last_date": "1942-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1943,
      "value": 11.67,
      "last_date": "1943-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1944,
      "value": 13.28,
      "last_date": "1944-12-29",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1945,
      "value": 17.360001,
      "last_date": "1945-12-31",
      "observations": 247,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1946,
      "value": 15.3,
      "last_date": "1946-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1947,
      "value": 15.3,
      "last_date": "1947-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1948,
      "value": 15.2,
      "last_date": "1948-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1949,
      "value": 16.790001,
      "last_date": "1949-12-30",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1950,
      "value": 20.43,
      "last_date": "1950-12-29",
      "observations": 249,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1951,
      "value": 23.77,
      "last_date": "1951-12-31",
      "observations": 249,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1952,
      "value": 26.57,
      "last_date": "1952-12-31",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1953,
      "value": 24.809999,
      "last_date": "1953-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1954,
      "value": 35.98,
      "last_date": "1954-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1955,
      "value": 45.48,
      "last_date": "1955-12-30",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1956,
      "value": 46.669998,
      "last_date": "1956-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1957,
      "value": 39.990002,
      "last_date": "1957-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1958,
      "value": 55.209999,
      "last_date": "1958-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1959,
      "value": 59.889999,
      "last_date": "1959-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1960,
      "value": 58.110001,
      "last_date": "1960-12-30",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1961,
      "value": 71.550003,
      "last_date": "1961-12-29",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1962,
      "value": 63.099998,
      "last_date": "1962-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1963,
      "value": 75.019997,
      "last_date": "1963-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1964,
      "value": 84.75,
      "last_date": "1964-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1965,
      "value": 92.43,
      "last_date": "1965-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1966,
      "value": 80.330002,
      "last_date": "1966-12-30",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1967,
      "value": 96.470001,
      "last_date": "1967-12-29",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1968,
      "value": 103.860001,
      "last_date": "1968-12-31",
      "observations": 226,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1969,
      "value": 92.059998,
      "last_date": "1969-12-31",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1970,
      "value": 92.150002,
      "last_date": "1970-12-31",
      "observations": 254,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1971,
      "value": 102.089996,
      "last_date": "1971-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1972,
      "value": 118.050003,
      "last_date": "1972-12-29",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1973,
      "value": 97.550003,
      "last_date": "1973-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1974,
      "value": 68.559998,
      "last_date": "1974-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1975,
      "value": 90.190002,
      "last_date": "1975-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1976,
      "value": 107.459999,
      "last_date": "1976-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1977,
      "value": 95.099998,
      "last_date": "1977-12-30",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1978,
      "value": 96.110001,
      "last_date": "1978-12-29",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1979,
      "value": 107.940002,
      "last_date": "1979-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1980,
      "value": 135.76,
      "last_date": "1980-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1981,
      "value": 122.55,
      "last_date": "1981-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1982,
      "value": 140.64,
      "last_date": "1982-12-31",
      "observations": 259,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1983,
      "value": 164.93,
      "last_date": "1983-12-30",
      "observations": 260,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1984,
      "value": 167.24,
      "last_date": "1984-12-31",
      "observations": 261,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1985,
      "value": 211.28,
      "last_date": "1985-12-31",
      "observations": 260,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1986,
      "value": 242.16,
      "last_date": "1986-12-31",
      "observations": 258,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1987,
      "value": 247.09,
      "last_date": "1987-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1988,
      "value": 277.72,
      "last_date": "1988-12-30",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1989,
      "value": 353.4,
      "last_date": "1989-12-29",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1990,
      "value": 330.22,
      "last_date": "1990-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1991,
      "value": 417.09,
      "last_date": "1991-12-31",
      "observations": 254,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1992,
      "value": 435.71,
      "last_date": "1992-12-31",
      "observations": 254,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1993,
      "value": 466.45,
      "last_date": "1993-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1994,
      "value": 461.17,
      "last_date": "1994-12-29",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1995,
      "value": 615.93,
      "last_date": "1995-12-29",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1996,
      "value": 740.74,
      "last_date": "1996-12-31",
      "observations": 254,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1997,
      "value": 970.43,
      "last_date": "1997-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1998,
      "value": 1229.23,
      "last_date": "1998-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 1999,
      "value": 1469.25,
      "last_date": "1999-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2000,
      "value": 1320.5,
      "last_date": "2000-12-29",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2001,
      "value": 1148.16,
      "last_date": "2001-12-31",
      "observations": 249,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2002,
      "value": 879.82,
      "last_date": "2002-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2003,
      "value": 1111.92,
      "last_date": "2003-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2004,
      "value": 1211.92,
      "last_date": "2004-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2005,
      "value": 1248.29,
      "last_date": "2005-12-30",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2006,
      "value": 1418.3,
      "last_date": "2006-12-29",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2007,
      "value": 1468.36,
      "last_date": "2007-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2008,
      "value": 903.25,
      "last_date": "2008-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2009,
      "value": 1115.1,
      "last_date": "2009-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2010,
      "value": 1257.64,
      "last_date": "2010-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2011,
      "value": 1257.6,
      "last_date": "2011-12-30",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2012,
      "value": 1426.19,
      "last_date": "2012-12-31",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2013,
      "value": 1848.36,
      "last_date": "2013-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2014,
      "value": 2058.9,
      "last_date": "2014-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2015,
      "value": 2043.94,
      "last_date": "2015-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2016,
      "value": 2238.83,
      "last_date": "2016-12-30",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2017,
      "value": 2673.61,
      "last_date": "2017-12-29",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2018,
      "value": 2506.85,
      "last_date": "2018-12-31",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2019,
      "value": 3230.78,
      "last_date": "2019-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2020,
      "value": 3756.08,
      "last_date": "2020-12-31",
      "observations": 253,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2021,
      "value": 4766.19,
      "last_date": "2021-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2022,
      "value": 3839.49,
      "last_date": "2022-12-30",
      "observations": 251,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2023,
      "value": 4769.82,
      "last_date": "2023-12-29",
      "observations": 250,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2024,
      "value": 5881.62,
      "last_date": "2024-12-31",
      "observations": 252,
      "source_kind": "ASSET",
      "source_id": "1764"
    },
    {
      "year": 2025,
      "value": 6845.5,
      "last_date": "2025-12-31",
      "observations": 250,
      "source_kind": "INDICATOR",
      "source_id": "211"
    },
    {
      "year": 2026,
      "value": 7600.5,
      "last_date": "2026-08-03",
      "observations": 146,
      "source_kind": "INDICATOR",
      "source_id": "211"
    }
  ]
};
