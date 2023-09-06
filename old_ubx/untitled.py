# import required package
import pygmaps
  
# maps method return map object
# 1st argument is center latitude
# 2nd argument is center longitude
# 3ed argument zoom level
mymap1 = pygmaps.maps(16.723503113, 120.781272888, 15)
  
# create the HTML file which includes
# google map. Pass the absolute path
# as an argument.
mymap1.draw('map.html')





# # import gmplot package
# import gmplot
  
# # GoogleMapPlotter return Map object
# # Pass the center latitude and
# # center longitude
# gmap1 = gmplot.GoogleMapPlotter(16.723503113, 120.781272888, 15)
  
# # Pass the absolute path
# gmap1.draw("C:\\Users\Ket\Desktop") 