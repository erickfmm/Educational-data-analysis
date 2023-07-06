mkdir escolar_rendimiento
cd escolar_rendimiento

curl https://datosabiertos.mineduc.cl/wp-content/uploads/2023/02/Rendimiento-2022.rar --output 2022.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2022/04/Rendimiento-2021.rar --output 2021.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2020.rar --output 2020.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2019.rar --output 2019.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2018.rar --output 2018.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2017.rar --output 2017.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2016.rar --output 2016.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2015.rar --output 2015.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2014.rar --output 2014.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2013.rar --output 2013.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2012.rar --output 2012.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2011.rar --output 2011.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2010.rar --output 2010.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2009.rar --output 2009.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2008.rar --output 2008.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2007.rar --output 2007.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2006.rar --output 2006.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2005.rar --output 2005.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2004.rar --output 2004.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2003.rar --output 2003.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2002.rar --output 2002.rar
for file in *.rar; do unrar e "$file"; done
cd ..