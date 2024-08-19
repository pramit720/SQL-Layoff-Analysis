-- 1. REMOVE DUPLICATE

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * from layoffs_staging;


INSERT INTO layoffs_staging
SELECT * from layoffs;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
where row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as row_num
FROM layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num > 1;


SELECT * FROM layoffs_staging2
WHERE row_num > 1;



-- Standardize data

SELECT company, trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

SELECT DISTINCT(industry)
FROM layoff s_staging2
ORDER BY 1;


SELECT *
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%"; 


SELECT DISTINCT(country)
FROM layoffs_st aging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = "United States"
WHERE COUNTRY LIKE "United States%";

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Didn't work UPDATE layoffs_staging2
-- SET `date` = str_to_date(`date`, '%m/%d/%Y'); 

-- Dealing with NULL values

UPDATE layoffs_staging2
SET `date` = CASE
	WHEN `date` IS NULL THEN NULL
ELSE 
str_to_date(`date`, '%m/%d/%Y')
END;

UPDATE layoffs_staging2
SET `date` = CASE
WHEN `date` = 'NULL'
THEN NULL
ELSE STR_TO_DATE(`date`, '%m/%d/%Y')
END;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = NULL
WHERE company = 'NULL';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''; 

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND 
t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND 
t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND
percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND
percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off INT;

Alter TABLE layoffs_staging2
MODIFY COLUMN funds_raised_millions INT;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(funds_raised_millions)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`) 
ORDER BY 2  DESC;  

SELECT *
FROM layoffs_staging2;


SELECT substring(`date`,1,7 ) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`,1,7 )  IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC;

WITH rolling_total AS
(
SELECT substring(`date`,1,7 ) AS `MONTH`, SUM(total_laid_off) as total_off
FROM layoffs_staging2
WHERE substring(`date`,1,7 )  IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC
)

SELECT `Month`, total_off, sum(total_off) OVER(ORDER BY `MONTH`) AS rolling_sum
FROM rolling_total;


SELECT * FROM layoffs_staging2;

SELECT company, YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC; 


WITH company_year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
),
company_year_rank  AS
(
SELECT *,
DENSE_RANK () OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL
AND 
total_laid_off is NOT NULL)

SELECT * 
 FROM company_year_rank
 WHERE ranking <= 5
 ORDER BY ranking ;

