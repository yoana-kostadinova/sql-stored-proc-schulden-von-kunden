-- have in mind some parts of the code are deleted purpously
-- stored procedure for SAP HANA
CREATE PROCEDURE DebtsFromCustomers (IN CardName NVARCHAR(100))

LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS

CardCode NVARCHAR(15);
IsExists int := 0;

CURSOR cursor1 
FOR
SELECT "CardCode"
FROM OCRD
WHERE "CardType" = 'C';

BEGIN

	SELECT COUNT(*) INTO IsExists 
	FROM M_TEMPORARY_TABLES
	WHERE TABLE_NAME = '#TEMP_TABLE_1' AND SCHEMA_NAME = CURRENT_SCHEMA and connection_id = current_connection; 
	
	IF :IsExists > 0 THEN
		DROP TABLE #TEMP_TABLE_1;
	END IF;
	
	CREATE LOCAL TEMPORARY COLUMN TABLE #TEMP_TABLE_1 
	(
		"CardCode" NVARCHAR(15),
		"CardName" NVARCHAR(100),
		"DocType" NVARCHAR(5),
		"DocNum" INT,
		"DocDate" DATE, 
		"DueDate" DATE, 
		"Total" DECIMAL(19,6), 
		"Payment Type" NVARCHAR(100), 
		"Remaining" DECIMAL(19,6), 
		"SalesEmployee1" NVARCHAR(100), 
		"SalesEmployee2" NVARCHAR(100), 
		"Office" NVARCHAR(100)		
	);	
	
	OPEN cursor1;
	FETCH cursor1 into CardCode;	
	IF (:CardName IS NOT NULL)
	THEN
	SELECT "CardCode" into CardCode 
	FROM OCRD
	WHERE "CardName" = CardName;	
	END IF;
	
	WHILE NOT cursor1::NOTFOUND 
		DO	
		INSERT INTO #TEMP_TABLE_1
			(
				SELECT
				DISTINCT
				T0."CardCode", 
				T0."CardName",
				NULL,
				NULL,
				NULL,
				NULL,
				NULL,
				NULL,
				NULL,
				NULL,
				NULL,
				NULL
				FROM OCRD T0
				WHERE T0."CardCode" = CardCode
							
				UNION ALL
				
				SELECT
				DISTINCT
				NULL, 
				NULL,
				'INV',
				--T1."DocEntry",
				T1."DocNum",
				T1."DocDate",
				T1."DocDueDate",
				T1."DocTotal",
					CASE 				 
						WHEN T3."CashSum" <> 0 THEN 'Cash'
						WHEN T3."CheckSum" <> 0 THEN 'Check' 
						WHEN T3."TrsfrSum" <> 0 THEN 'Bank Transfer'  
					END,
				T1."DocTotal" - T1."PaidToDate",
				T5."firstName" || T5."lastName",
				T6."firstName" || T6."lastName",
				--*
				FROM OINV T1
				LEFT JOIN RCT2 T2
				ON T1."DocEntry" = T2."DocEntry" 
				LEFT JOIN ORCT T3
				ON T3."DocEntry" = T2."DocNum" AND T2."InvType" = 13
				LEFT JOIN INV12 T4
				ON T4."DocEntry" = T1."DocEntry"
				--*
				WHERE T1."CardCode" = CardCode AND (T1."PaidToDate" < T1."DocTotal" OR T1."DocTotal" < 0) 
				
				UNION ALL
				
				SELECT
				DISTINCT
				NULL,
				NULL, 
				'RIN',
				--T1."DocEntry",
				T1."DocNum",
				T1."DocDate",
				T1."DocDueDate",
				T1."DocTotal",
					CASE 				 
						WHEN T3."CashSum" <> 0 THEN 'Cash'
						WHEN T3."CheckSum" <> 0 THEN 'Check' 
						WHEN T3."TrsfrSum" <> 0 THEN 'Bank Transfer'  
					END,
				T1."DocTotal" - T1."PaidToDate",
				T5."firstName" || T5."lastName",
				T6."firstName" || T6."lastName",
				--*
				FROM ORIN T1
				LEFT JOIN RCT2 T2
				ON T1."DocEntry" = T2."DocEntry" AND T2."InvType" = 14
				LEFT JOIN ORCT T3
				ON T3."DocEntry" = T2."DocNum"
				--*
				WHERE T1."CardCode" = CardCode AND (T1."PaidToDate" < T1."DocTotal" OR T1."DocTotal" < 0) 
			
				UNION ALL 
				
				SELECT
				DISTINCT
				NULL, 
				NULL,
				'RC',
				--T1."DocEntry",
				T1."DocNum",
				--T1."NoDocSum",
				T1."DocDate",
				T1."DocDueDate",
				T1."DocTotal" - T1."NoDocSum",
					CASE 				 
						WHEN T1."CashSum" <> 0 THEN 'Cash'
						WHEN T1."CheckSum" <> 0 THEN 'Check' 
						WHEN T1."TrsfrSum" <> 0 THEN 'Bank Transfer'  
					END,
				T1."NoDocSum",
				NULL,
				NULL,
				NULL
				FROM ORCT T1
				WHERE T1."CardCode" = CardCode AND T1."NoDocSum" <> 0
				
				UNION ALL
				
				SELECT
				DISTINCT
				NULL, 
				NULL,
				'CH',
				T2."DocNum",			
				T2."DocDate",
				T2."DocDueDate",
				T2."DocTotal",
					CASE 				 
						WHEN T2."CashSum" <> 0 THEN 'Cash'
						WHEN T2."CheckSum" <> 0 THEN 'Check' 
						WHEN T2."TrsfrSum" <> 0 THEN 'Bank Transfer'  
					END,
				T2."DocTotal", 
				NULL,
				NULL,
				NULL
				FROM OCHH T1
				LEFT OUTER JOIN ORCT T2
				ON T1."RcptNum" = T2."DocEntry"
				WHERE T1."CardCode" = CardCode AND T1."Deposited" = 'N' 	
				
				UNION ALL
				
				SELECT  
				DISTINCT
				NULL, 
				NULL,
				'JE',
				T2."Number",
				T2."RefDate",
				T2."DueDate",
					CASE 				 
						WHEN T1."Debit" <> 0 THEN T1."Debit"
						WHEN T1."Credit" <> 0 THEN 0 - T1."Credit"
					END,
				T1."ContraAct",
				NULL,
				NULL,
				NULL,
				NULL
				FROM JDT1 T1
				LEFT OUTER JOIN OJDT T2
				ON T1."TransId" = T2."TransId"
				WHERE T1."ShortName" = CardCode AND (T1."TransType" = 30 OR T1."TransType" = -2)
				
			); 
			
			IF (CardName IS NOT NULL) 
			THEN
				BREAK; 
			END IF;	
			
			IF (CardName IS NULL)
			THEN
				FETCH cursor1 into CardCode; 
			END IF;				
			
	END WHILE; 
	CLOSE cursor1;	
		
	SELECT *
	FROM #TEMP_TABLE_1;

END;