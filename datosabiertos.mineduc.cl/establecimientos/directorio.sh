mkdir directorio_establecimientos
cd directorio_establecimientos

curl https://datosabiertos.mineduc.cl/wp-content/uploads/2022/09/Directorio-oficial-EE-2022.rar --output 2022.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2021.rar --output 2021.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2020.rar --output 2020.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2019.rar --output 2019.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2018.rar --output 2018.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2017.rar --output 2017.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2016.rar --output 2016.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-Oficial-EE-2015.rar --output 2015.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2014.rar --output 2014.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio-oficial-EE-2013.rar --output 2013.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2012.csv --output 2012.csv
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2011.csv --output 2011.csv
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2010.csv --output 2010.csv
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2009.csv --output 2009.csv
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2008.csv --output 2008.csv
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2007.csv --output 2007.csv
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2006.csv --output 2006.csv
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2005.csv --output 2005.csv
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Directorio_oficial_EE_2004.csv --output 2004.csv
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial2003.zip --output 2003.zip
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial2002.zip --output 2002.zip
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial2001.zip --output 2001.zip
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial2000.zip --output 2000.zip
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1999.zip --output 1999.zip
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1998.zip --output 1998.zip
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1997.rar --output 1997.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1996.rar --output 1996.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1995.rar --output 1995.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1994.rar --output 1994.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1993.rar --output 1993.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/DirectorioOficial1992.rar --output 1992.rar
unrar x *.rar
unzip x *.zip
cd ..