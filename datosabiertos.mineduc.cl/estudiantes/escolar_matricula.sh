mkdir escolar_matricula
cd escolar_matricula
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2022/09/Matricula-por-estudiante-2022.rar --output 2022.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2021.rar --output 2021.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2020.rar --output 2020.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2019.rar --output 2019.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2018.rar --output 2018.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2017.rar --output 2017.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2016.rar --output 2016.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2015.rar --output 2015.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2014.rar --output 2014.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2013.rar --output 2013.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2012.rar --output 2012.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2011.rar --output 2011.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2010.rar --output 2010.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2022/01/Matricula-por-estudiante-2009.rar --output 2009.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2008.rar --output 2008.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2007.rar --output 2007.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2006.rar --output 2006.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2005.rar --output 2005.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-por-estudiante-2004.rar --output 2004.rar
for file in *.rar; do unrar e "$file"; done
cd ..