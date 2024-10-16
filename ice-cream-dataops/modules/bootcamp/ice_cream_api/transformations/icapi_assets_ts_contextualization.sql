SELECT ts.externalId AS externalId,
       ast.id AS assetId
FROM `_cdf`.`timeseries` AS ts
INNER JOIN `_cdf`.`assets` AS ast ON split(ts.externalId, ":") [0] = ast.externalId