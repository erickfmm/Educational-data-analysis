mkdir escolar_rendimiento
cd escolar_rendimiento

curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2023/02/Rendimiento-2022.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2022/04/Rendimiento-2021.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2020.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2019.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2018.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2017.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2016.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2015.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2014.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2013.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2012.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2011.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2010.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2009.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2008.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2007.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2006.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2005.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2004.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2003.rar
curl -LO https://datosabiertos.mineduc.cl/wp-content/uploads/2021/12/Rendimiento-2002.rar
for file in *.rar; do unrar e "$file"; done
cd ..