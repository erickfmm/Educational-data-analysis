mkdir directorio_establecimientos
cd directorio_establecimientos

curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2022/09/Directorio-oficial-EE-2022.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2021.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2020.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2019.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2018.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2017.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2016.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-Oficial-EE-2015.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2014.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2013.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2012.csv
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2011.csv
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2010.csv
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2009.csv
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2008.csv
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2007.csv
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2006.csv
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2005.csv
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2004.csv
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial2003.zip
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial2002.zip
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial2001.zip
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial2000.zip
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1999.zip
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1998.zip
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1997.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1996.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1995.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1994.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1993.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1992.rar
unrar x -c- -cfg- -inul -o+ -y "%CD%\*.rar" "%CD%\"
cd ..