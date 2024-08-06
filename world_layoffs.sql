SELECT * FROM layoffs;

CREATE TABLE layoffs_stagging LIKE layoffs;

INSERT layoffs_stagging 
SELECT * FROM layoffs;

SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions)
AS row_num FROM layoffs_stagging;

WITH duplicate_cte AS
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions)
AS row_num FROM layoffs_stagging
)
SELECT * FROM duplicate_cte WHERE row_num >1;

CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_stagging2
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions)
AS row_num FROM layoffs_stagging;

SELECT * FROM layoffs_stagging2 WHERE row_num >1;

DELETE
FROM layoffs_stagging2 WHERE row_num >1;

SELECT * FROM layoffs_stagging2;

SELECT company, TRIM(company) FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET company = TRIM(company);

SELECT * FROM layoffs_stagging2
WHERE industry LIKE 'Crypto';

UPDATE layoffs_stagging2 SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_stagging2 ORDER BY 1;

UPDATE layoffs_stagging2 SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date` FROM layoffs_stagging2;

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_stagging2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_stagging2 WHERE total_laid_off is NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_stagging2 WHERE industry IS NULL OR
industry = '';

SELECT * FROM layoffs_stagging2 t1
JOIN layoffs_stagging2 t2 ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_stagging2
SET industry = NULL WHERE industry = '';

SELECT t1.industry, t2.industry FROM layoffs_stagging2 t1
JOIN layoffs_stagging2 t2 ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_stagging2 t1
JOIN layoffs_stagging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

DELETE FROM layoffs_stagging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_stagging2;

ALTER TABLE layoffs_stagging2 DROP COLUMN row_num;

SELECT MAX(total_laid_off) highest_laid_off, MAX(percentage_laid_off) percent
FROM layoffs_stagging2;

SELECT * FROM layoffs_stagging2 WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off) total_laid_off FROM layoffs_stagging2
GROUP BY company ORDER BY 2 DESC;

SELECT MIN(`date`) earliest_date, MAX(`date`) latest_date FROM layoffs_stagging2;

SELECT industry, SUM(total_laid_off) total_laid_off FROM layoffs_stagging2
GROUP BY industry ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off) total_laid_off FROM layoffs_stagging2
GROUP BY country ORDER BY 2 DESC;

SELECT YEAR(`date`) year, SUM(total_laid_off) total_laid_off FROM layoffs_stagging2
GROUP BY YEAR(`date`) ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off) total_laid_off FROM layoffs_stagging2
GROUP BY stage ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) total
FROM layoffs_stagging2 WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH` ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) total
FROM layoffs_stagging2 WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH` ORDER BY 1 ASC
)
SELECT `MONTH`, total, SUM(total) OVER(ORDER BY `MONTH`) Rolling_total
FROM Rolling_Total;

SELECT company, YEAR(`date`) year, SUM(total_laid_off) total_laid_off FROM layoffs_stagging2
GROUP BY company, YEAR(`date`) ORDER BY 3 DESC;

WITH Company_year AS
(
SELECT company, YEAR(`date`) years, SUM(total_laid_off) total_laid_off FROM layoffs_stagging2
GROUP BY company, YEAR(`date`)
)
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) ranking
FROM Company_year WHERE years IS NOT NULL ORDER BY ranking ASC;