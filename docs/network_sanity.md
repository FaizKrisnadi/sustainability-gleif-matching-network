# Network Sanity Checks

## Metadata
- Generated at (UTC): `2026-02-27T17:40:10Z`
- Inputs: `data/processed/edges.csv`, `data/processed/nodes.csv`
- Directed assumption for sanity stats: treat `firm_i` as parent and `firm_j` as child.

## Summary Metrics
- n_edges (exact duplicates dropped): `634108`
- n_nodes_total (`nodes.csv`): `3219530`
- n_nodes_in_network (unique LEIs in edges): `419236`
- n_self_loops (`firm_i == firm_j`): `6429`
- missing_node_info_count (edge LEIs absent from `nodes.csv`): `86`
- nodes_in_network_file_created (this run): `false`

## Undirected Degree Summary
- min: `0`
- median: `1.0`
- max: `5309`

## Top 20 Out-Degree (firm_i as parent)
| parent_lei | out_degree |
|---|---:|
| 213800OYS5CYQ7BW8K97 | 7 |
| 81560097DD19407C3F80 | 7 |
| 8156006711DAEC1C0D75 | 6 |
| 213800O177ZTFGMLO414 | 6 |
| 9598007LNBDN3LQBH351 | 6 |
| 5299004BYUT68M9WBP64 | 6 |
| 213800MAI2RVVNANUA62 | 6 |
| 213800Z61IACVIM9AE42 | 6 |
| 529900201VGYOC11PH23 | 6 |
| 2138004WPXEIJZCZCK04 | 6 |
| 8156003B025E8C7A2D02 | 6 |
| 213800XEND33T1PEMK68 | 6 |
| 213800XEQIVBOWAX7278 | 6 |
| 969500ZRRUKQR7IEA748 | 6 |
| 815600E5D4D316FC4420 | 6 |
| 969500AWC3GWZRAT8G14 | 6 |
| 213800VBFE5G7LH67L96 | 6 |
| 81560057AE1FA6AAF369 | 6 |
| 8156005AF68BDFAC1945 | 6 |
| 253400QJ9FJ1W4HEEX74 | 6 |

## Top 20 In-Degree (firm_j as child)
| child_lei | in_degree |
|---|---:|
| 549300300IVF7LGDV529 | 5309 |
| 5493009GCYS5Q8HOI372 | 4106 |
| DQ2T0MMUTO0IPF9G9Z35 | 2578 |
| 784F5XWPLTWKTBV3E584 | 2392 |
| 7LTWFZYICNSX8D621K86 | 1362 |
| W51AX6427FJZJFPF8H34 | 1288 |
| 549300TDFL442EPSLM98 | 1282 |
| IGJSJL3JD5P30I6NJZ34 | 1116 |
| 969500WCY7P9HOJNP063 | 1106 |
| 549300RK1FB0VMTPD087 | 1094 |
| 5493005EBZDOXNFCKY36 | 1009 |
| 969500BXTBE4U1HLLF51 | 998 |
| 5493003QNP1E68GGMZ05 | 889 |
| 5493001Z012YSB2A0K51 | 829 |
| 2221002W65PS26SQKX63 | 807 |
| 6SHGI4ZSSLCXXQSBB395 | 796 |
| 529900NZCIJDWLUHCS06 | 784 |
| 635400XKS4TFYYAQ1K41 | 781 |
| 5493004330BCAPB3GT42 | 776 |
| 9DJT3UXIJIZJI4WXO774 | 773 |
