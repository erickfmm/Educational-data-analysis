mkdir parvularia_matricula
cd parvularia_matricula
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2022/10/Matricula-Ed-Parvularia-2022.rar --output 2022.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2021.rar --output 2021.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2020.rar --output 2020.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2019.rar --output 2019.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2018.rar --output 2018.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2017.rar --output 2017.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2016.rar --output 2016.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2015.rar --output 2015.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2014.rar --output 2014.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2013.rar --output 2013.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2012.rar --output 2012.rar
curl https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Matricula-Ed.-Parvularia-2011.rar --output 2011.rar
unrar x *.rar
cd ..