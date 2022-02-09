import numpy as np
import gdal
import os
import boto3
from botocore.exceptions import ClientError
import json
import logging
# import sys

s3 = boto3.client('s3')



#%%
def lambda_handler(event, context):
    
    body = json.loads(event['body'])
    json_file = body
    
    #get  input json and extract geojson
    try:
        project_id = json_file["project_id"]
        ROI = json_file["ROI"]
    except Exception as e:
        print(e)
        print("Input JSON field have an error.")


    #for local
    path_to_tmp = "/home/christos/Desktop/SCiO_Projects/lup4ldn/data/cropped_files/"
    #for aws
    # path_to_tmp = "/tmp/"
    
    # s3_file_path = '/vsis3/lup4ldn-dataset' + "/" + country_iso + "/"
    # s3_file_path = "https://lup4ldn-default-global-datasets.s3.eu-central-1.amazonaws.com/"
    s3_file_path = '/vsis3/lup4ldn-default-global-datasets/'
    
    
    path_to_land_degradation = s3_file_path  + "global_land_degradation_map.tif"
    path_to_land_cover_folder = s3_file_path  + "global_land_cover_dataset/"
    path_to_land_use = s3_file_path + "global_land_use_map.tif"
    path_to_land_suitability = s3_file_path + "global_land_suitability_map.tif"
    
    
    ##crop land degradation (SDG)
    save_land_degradation_file = path_to_tmp + "cropped_land_degradation.tif"
    
    gdal_warp_kwargs_target_area = {
        'format': 'GTiff',
        'cutlineDSName' : json.dumps(ROI),
        'cropToCutline' : True,
        'height' : None,
        'width' : None,
        'srcNodata' : -32768.0,
        'dstNodata' : -32768.0,
        'creationOptions' : ['COMPRESS=DEFLATE']
    }
    
    try:
        gdal.Warp(save_land_degradation_file,path_to_land_degradation,**gdal_warp_kwargs_target_area)
    except Exception as e:
        print(e)
        print("if 'returned NULL without setting an error', probably at least one of the file paths is wrong")
        
        
    try:
        land_degradation_tif = gdal.Open(save_land_degradation_file)
        land_degradation_array = land_degradation_tif.ReadAsArray()
        x_ref = land_degradation_tif.RasterXSize
        y_ref = land_degradation_tif.RasterYSize
    except Exception as e:
        print(e)
        print("if ''NoneType' object has no attribute', probably the file path is wrong")
        
        
    gdal_warp_kwargs_target_area["height"] = y_ref
    gdal_warp_kwargs_target_area["width"] = x_ref
    
    
    ## land use
    save_land_use_file = path_to_tmp + "cropped_land_use.tif"
    
    try:
        gdal.Warp(save_land_use_file,path_to_land_use,**gdal_warp_kwargs_target_area)
    except Exception as e:
        print(e)
        print("if 'returned NULL without setting an error', probably at least one of the file paths is wrong")
        
    #must use gdal.Open in order to fill the file created from gdal.Warp, else the file remaines full of nodata
    try:
        t = gdal.Open(save_land_use_file)
    except Exception as e:
        print(e)
        print("if ''NoneType' object has no attribute', probably the file path is wrong")
      
    
    ## land suitability
    save_suitability_file = path_to_tmp + "cropped_suitability.tif"
    
    try:
        gdal.Warp(save_suitability_file,path_to_land_suitability,**gdal_warp_kwargs_target_area)
    except Exception as e:
        print(e)
        print("if 'returned NULL without setting an error', probably at least one of the file paths is wrong")
        
    #must use gdal.Open in order to fill the file created from gdal.Warp, else the file remaines full of nodata
    try:
        land_suitability_tif = gdal.Open(save_suitability_file)
        land_suitability_array = land_suitability_tif.ReadAsArray()
    except Exception as e:
        print(e)
        print("if ''NoneType' object has no attribute', probably the file path is wrong")
    
        # return
    
    ## land cover
    #read the first year
    save_land_cover_file = path_to_tmp + "cropped_land_cover.tif"
    
    try:
        #CHANGE HERE THE YEAR IF MORE YEARS ARE TO BE USED
        gdal.Warp(save_land_cover_file,path_to_land_cover_folder + "global_land_cover_map_2020.tif" ,**gdal_warp_kwargs_target_area)
    except Exception as e:
        print(e)
        print("if 'returned NULL without setting an error', probably at least one of the file paths is wrong")
        
    #must use gdal.Open in order to fill the file created from gdal.Warp, else the file remaines full of nodata
    try:
        land_cover_tif = gdal.Open(save_land_cover_file)
        land_cover_array = land_cover_tif.ReadAsArray()
    except Exception as e:
        print(e)
        print("if ''NoneType' object has no attribute', probably the file path is wrong")
    
    land_cover_array = np.expand_dims(land_cover_array,axis=0)
    
    
    
    # # read and concatenate the rest years, IF we want older years as well
    # for i in range(2019,2021):
    #     try:
    #         gdal.Warp(save_land_cover_file,path_to_land_cover_folder + "global_land_cover_map_" + str(i) + ".tif" ,**gdal_warp_kwargs_target_area)
    #     except Exception as e:
    #         print(e)
    #         print(i)
    #         print("if 'returned NULL without setting an error', probably at least one of the file paths is wrong")
    
    #     #must use gdal.Open in order to fill the file created from gdal.Warp, else the file remaines full of nodata
    #     try:
    #         temp_array = gdal.Open(save_land_cover_file).ReadAsArray()
    #     except Exception as e:
    #         print(e)
    #         print("if ''NoneType' object has no attribute', probably the file path is wrong")
        
    #     temp_array = np.expand_dims(temp_array,axis=0)
    #     land_cover_array = np.concatenate((land_cover_array, temp_array), axis=0)
        
    ## map the 22-classes lc to the 7-classes lc
    # Functions
    def save_arrays_to_tif(output_tif_path, array_to_save, old_raster_used_for_projection):
        # output_tif_path : path to output file including its title in string format.
        # array_to_save : numpy array to be saved, 3d shape with the following format (no_bands, width, height). If only one band then should be extended with np.expand_dims to the format (1, width, height).
        # reference_tif : path to tif which will be used as reference for the geospatial information applied to the new tif.

        if len(array_to_save.shape)==2:
            array_to_save = np.expand_dims(array_to_save,axis=0)

        no_bands, width, height = array_to_save.shape

        gt = old_raster_used_for_projection.GetGeoTransform()
        wkt_projection = old_raster_used_for_projection.GetProjectionRef()

        driver = gdal.GetDriverByName("GTiff")
        DataSet = driver.Create(output_tif_path, height, width, no_bands, gdal.GDT_Int16,['COMPRESS=LZW']) #gdal.GDT_Int16

        DataSet.SetGeoTransform(gt)
        DataSet.SetProjection(wkt_projection)

        #no data value
        ndval = -32768
        for i, image in enumerate(array_to_save, 1):
            DataSet.GetRasterBand(i).WriteArray(image)
            DataSet.GetRasterBand(i).SetNoDataValue(ndval)
        DataSet = None
        # print(output_tif_path, " has been saved")
        return

    def map_land_cover_to_trendsearth_labels(array,labels_dict):
        for key in labels_dict:
            array = np.where(array==key,labels_dict[key],array)
        return array
    
    dict_labels_map_100m_to_trends = {
        10 : 3,
        11 : 3,
        12 : 3,
        20 : 3,
        30 : 3,
        40 : 2,
        50 : 1,
        60 : 1,
        61 : 1,
        62 : 1,
        70 : 1,
        71 : 1,
        72 : 1,
        80 : 1,
        81 : 1,
        82 : 1,
        90 : 1,
        100 : 1,
        110 : 2,
        120 : 2,
        121 : 2,
        122 : 2,
        130 : 2,
        140 : 2,
        150 : 2,
        151 : 2,
        152 : 2,
        153 : 2,
        160 : 4,
        170 : 4,
        180 : 4,
        190 : 5,
        200 : 6,
        201 : 6,
        202 : 6,
        210 : 7,
        220 : 6,
        0 : -32768
    }
    land_cover_array = map_land_cover_to_trendsearth_labels(land_cover_array,dict_labels_map_100m_to_trends)
    
    unique, counts = np.unique(land_cover_array, return_counts = True)
    lc_hectares = dict(zip([str(x) for x in unique], 9 * [int(x) for x in counts]))
    
        
    save_arrays_to_tif(save_land_cover_file,land_cover_array,land_cover_tif)
    
    
    # future land degradation map
    
    #!!!!!!!!!!!WATCH OUT FOR NEGATIVE OVERFLOW IF -1 FROM LAND DEGRADATION IS ADDED TO -32768 NO DATA OF SUITABILITY!!!!!!!!!!!!
    future_ld_map = 10*land_suitability_array + land_degradation_array
    
    future_ld_map = np.where(future_ld_map<-1,-32768,future_ld_map )

    future_ld_map = np.where(future_ld_map==-1,5,future_ld_map )
    
    future_ld_map = np.where(future_ld_map==0,2,future_ld_map )
    
    future_ld_map = np.where(future_ld_map==1,1,future_ld_map )

    future_ld_map = np.where(np.logical_or(future_ld_map==10,future_ld_map==11),1,future_ld_map )

    future_ld_map = np.where(np.logical_or(future_ld_map==20,future_ld_map==21),2,future_ld_map )

    future_ld_map = np.where(np.logical_or(future_ld_map==30,future_ld_map==31),3,future_ld_map )

    future_ld_map = np.where(future_ld_map==9,4,future_ld_map )

    future_ld_map = np.where(np.logical_or(future_ld_map==19,future_ld_map==29),5,future_ld_map)
    
    save_future_ld_map_file = path_to_tmp + "cropped_future_ld.tif"
    
    save_arrays_to_tif(save_future_ld_map_file,future_ld_map,land_cover_tif)
    
    #upload files
    file_to_upload = os.listdir(path_to_tmp)
    
    s3_lambda_path = "https://lup4ldn-lambdas.s3.eu-central-1.amazonaws.com/"    

    # for file in file_to_upload:
    #     path_to_file_for_upload = path_to_tmp + file
    #     target_bucket = "lup4ldn-lambdas"
    
    #     object_name = project_id +  "/" + file
        
    #     # Upload the file
    #     try:
    #         response = s3.upload_file(path_to_file_for_upload, target_bucket, object_name)
    # #         print("Uploaded file: " + file)
    #     except ClientError as e:
    #         logging.error(e)

    my_output = {
    "land_cover" : s3_lambda_path + project_id + "/cropped_land_cover.tif",
    "land_use" : s3_lambda_path + project_id + "/cropped_land_use.tif",
    "land_degradation" : s3_lambda_path + project_id + "/cropped_land_degradation.tif",
    "suitability" : s3_lambda_path + project_id + "/cropped_suitability.tif",
    "future_ld" : s3_lambda_path + project_id + "/cropped_future_ld.tif",
    "land_cover_hectares_per_class" : lc_hectares
    }
    
    
    return {
        "statusCode": 200,
        "body": json.dumps(my_output)
    }
    
#%%
input_json = {
    "body" : "{\"project_id\":\"some_ID\",\"ROI\":{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[9.70484734,33.30070114],[9.68963242,33.312229159999994],[9.67280293,33.325630190000005],[9.657265659999998,33.351661679999985],[9.63356495,33.3780899],[9.624302859999998,33.400100710000004],[9.62345886,33.40210342],[9.62186241,33.405899049999995],[9.621055599999998,33.40826035],[9.61611271,33.42273712000001],[9.615759850000002,33.42377090000001],[9.61461639,33.424610140000006],[9.60463905,33.431903840000004],[9.601172449999998,33.434440609999996],[9.58703041,33.440959930000005],[9.56937981,33.45106124999999],[9.56922913,33.45114899],[9.5563879,33.45845413],[9.54988194,33.46215820000001],[9.547912599999998,33.46614456],[9.54616261,33.46969223000001],[9.54361725,33.47484969999999],[9.524024959999998,33.49319839000001],[9.51667595,33.50121307],[9.51637554,33.50154114],[9.52301311,33.50976944],[9.52516556,33.5109024],[9.53619766,33.51670837],[9.54020691,33.52315903000001],[9.53976154,33.52632523000001],[9.538873669999996,33.53263855],[9.538501739999996,33.54713821000001],[9.538973809999998,33.55958175999999],[9.546034810000002,33.56694031000001],[9.55673313,33.57815933000001],[9.56929302,33.58732986],[9.57668018,33.59331894000002],[9.57820034,33.59455109],[9.574128149999998,33.59662628],[9.56998158,33.59873962000001],[9.55499554,33.60525131000001],[9.54187489,33.609199520000004],[9.54051208,33.610939030000004],[9.538996699999998,33.61286926],[9.53129959,33.62268066],[9.52356529,33.6375885],[9.51408768,33.64400101],[9.502639770000002,33.64416885],[9.49363232,33.64356232000001],[9.49296093,33.64351654000001],[9.48609829,33.64305115],[9.46917534,33.639678960000005],[9.45538712,33.64722061],[9.45318508,33.65980147999999],[9.453517910000002,33.66601944000001],[9.45153046,33.67773056],[9.44262791,33.68656921],[9.43334484,33.6928215],[9.4252634,33.70114899000001],[9.40927696,33.70365143],[9.401228899999998,33.7036171],[9.39516258,33.70359039000001],[9.386430740000002,33.69895935000001],[9.38106632,33.70249939000001],[9.37423801,33.70825958],[9.364585880000002,33.71120071],[9.354537010000003,33.71294022000001],[9.348718639999998,33.71957016],[9.344901079999998,33.72935104000001],[9.33915806,33.74531937000001],[9.33734989,33.76693344000001],[9.33725929,33.768016820000014],[9.3371954,33.76877975000001],[9.33394241,33.77465057],[9.327034,33.787120820000005],[9.325436590000004,33.79000092000001],[9.31324863,33.81043243],[9.312595370000002,33.81152724999999],[9.30677128,33.82128905999999],[9.291663169999998,33.83226013],[9.27906418,33.853450779999996],[9.26174355,33.874740599999996],[9.25354862,33.8884201],[9.252537730000002,33.89264679],[9.25004959,33.90304184],[9.247361179999997,33.91854095],[9.247219089999998,33.92392349],[9.24705601,33.930130000000005],[9.24696255,33.93365479],[9.24688911,33.93648911],[9.247275349999995,33.943748469999996],[9.248009679999996,33.95758056999999],[9.25171185,33.97077942],[9.252730370000002,33.98513031000001],[9.25511932,34.00400162],[9.25581264,34.01393127],[9.25626945,34.02045059],[9.25647259,34.02336884000001],[9.25257874,34.04268645999999],[9.252484320000002,34.04315948],[9.25257874,34.043670649999996],[9.25476837,34.055461879999996],[9.261611939999998,34.06491089000001],[9.26548862,34.068405150000004],[9.27235603,34.07460022],[9.27910042,34.07836914000001],[9.28988266,34.084400179999996],[9.30328941,34.095161440000005],[9.30937386,34.098072050000006],[9.31676292,34.10160828],[9.31839085,34.103382110000005],[9.32465458,34.11021042],[9.33498669,34.12091064000001],[9.34194469,34.12995148],[9.342256550000002,34.13035965],[9.33903408,34.13717270000001],[9.33844376,34.138420100000005],[9.32989311,34.14725113],[9.317025179999998,34.15584945999999],[9.30889988,34.163478850000004],[9.29204559,34.177009579999996],[9.28203297,34.186679840000004],[9.268528939999998,34.19577026000001],[9.255455970000002,34.21031189000001],[9.24207497,34.20378113],[9.223209379999998,34.19963837000001],[9.1917572,34.19804001000001],[9.18058777,34.19646835],[9.17205143,34.19527054000001],[9.15517426,34.19565964000001],[9.14210796,34.1893692],[9.12921715,34.18516159],[9.11899567,34.18753815],[9.116827959999998,34.18804169],[9.10438442,34.19419861000001],[9.098793979999998,34.195926670000006],[9.09280109,34.19778061],[9.063306810000002,34.19013977000001],[9.04080677,34.18262863],[9.02427673,34.17522812000001],[9.0141573,34.16587830000001],[9.00990868,34.15957642],[9.00814533,34.15695953],[9.01681042,34.14426422],[9.017223359999996,34.14366149999999],[9.0199337,34.13841629000001],[9.02086639,34.136615750000004],[9.02296925,34.13254929000001],[9.023634910000002,34.12377930000001],[9.023768419999998,34.12202072000001],[9.01849079,34.112625120000004],[9.018123629999998,34.111972810000005],[9.0173912,34.11066818],[9.01685715,34.11026764],[9.011075019999998,34.10591507000001],[9.009650230000002,34.10484314],[9.002788540000001,34.099681849999996],[8.994036670000002,34.0990715],[8.975082400000002,34.097751620000004],[8.949835779999999,34.10013580000001],[8.949350359999999,34.10018158],[8.92542362,34.104881289999994],[8.91029739,34.111480709999995],[8.90070915,34.11854172],[8.8947916,34.12266159],[8.8929987,34.123909000000005],[8.88441372,34.12234879000001],[8.87907124,34.121379850000004],[8.865491870000003,34.11143112],[8.8407402,34.10538864000001],[8.804689410000002,34.10963821000001],[8.775689130000002,34.11058044],[8.75166035,34.10971832000001],[8.730942729999999,34.10771179],[8.705136300000001,34.10351944000001],[8.685284610000002,34.09962845],[8.65879726,34.08919907],[8.65753174,34.08532715],[8.65681171,34.08311844000001],[8.65282536,34.07093048],[8.648135189999998,34.05060958999999],[8.638460159999998,34.03293991000001],[8.636326789999998,34.02843475],[8.63002491,34.01512909],[8.604843139999998,33.99365997],[8.5830574,33.96776962],[8.56256866,33.93947983],[8.547262189999998,33.92427444],[8.546727179999998,33.923740390000006],[8.542447090000001,33.920722960000006],[8.520901679999998,33.905521390000004],[8.486180310000002,33.8825798],[8.448945049999999,33.8615799],[8.44714928,33.860569],[8.39836693,33.827770230000006],[8.38700676,33.81926726999999],[8.38526344,33.81795883],[8.366164210000003,33.80366134999999],[8.35875511,33.798118590000016],[8.357860570000001,33.797431950000004],[8.33889103,33.78290176],[8.33024406,33.77627945],[8.327526090000001,33.774196620000005],[8.319593430000001,33.768119810000016],[8.31467724,33.76633453000001],[8.30782795,33.76385117],[8.30017757,33.75831985000001],[8.278165819999998,33.742401120000004],[8.26845169,33.73537827000001],[8.263732910000002,33.731883999999994],[8.23774529,33.71265030000001],[8.22529697,33.70497894000001],[8.22483063,33.70469284000001],[8.221824649999999,33.70283890000001],[8.21293068,33.697357180000004],[8.206448550000001,33.69336319000001],[8.19987965,33.68931580000001],[8.19910717,33.688838960000005],[8.168270109999998,33.672138210000014],[8.131531719999998,33.64749145999999],[8.092534070000003,33.623989110000004],[8.05716228,33.59590149],[8.02857685,33.574039459999995],[7.994502069999999,33.55184173999999],[7.975568770000001,33.539001459999994],[7.949862960000001,33.521568300000006],[7.908872130000001,33.4932785],[7.874187949999999,33.47293854000001],[7.84735394,33.45661163],[7.84685993,33.45644759999999],[7.83307695,33.45186614999999],[7.82761812,33.450054169999994],[7.82171392,33.448089599999996],[7.812520029999999,33.44530487],[7.8035121,33.44257355],[7.794935229999998,33.43997574000001],[7.790802,33.438720700000005],[7.75341892,33.43026352],[7.75466824,33.42899323000001],[7.75480509,33.42539215000001],[7.75765705,33.41645813],[7.75847292,33.409667969999994],[7.759543900000001,33.40449524],[7.76056099,33.400127409999996],[7.76149416,33.395816800000006],[7.763223170000001,33.387546539999995],[7.763438219999999,33.38656998],[7.76490211,33.37992859],[7.76677513,33.37531662000001],[7.76803112,33.37070847],[7.76921606,33.366470340000006],[7.7704649,33.36053467],[7.771348950000001,33.35656356999999],[7.77215099,33.352291109999996],[7.773393150000001,33.34692764],[7.77449512,33.342613220000004],[7.77542591,33.33808899],[7.776208880000001,33.33364486999999],[7.77742195,33.32713699000001],[7.77847385,33.323219300000005],[7.78013992,33.31793213],[7.78191519,33.31255722],[7.783092020000001,33.30878067],[7.78394985,33.30500793],[7.78487396,33.29932404000001],[7.7857151,33.29518509000001],[7.786608220000001,33.290237430000005],[7.78765392,33.28664017],[7.78877497,33.28395081000001],[7.79034281,33.27956772],[7.79206991,33.27601624000001],[7.793603900000001,33.27267838000001],[7.79498911,33.26900864000001],[7.797114849999998,33.264282230000006],[7.798936839999999,33.25985718],[7.800881859999999,33.255866999999995],[7.802775859999999,33.250732420000006],[7.80509806,33.247032170000004],[7.80683613,33.24251938000001],[7.80873394,33.23863602000001],[7.81042099,33.23468018],[7.81225204,33.231067659999994],[7.817455769999999,33.22133636000001],[7.82009315,33.21608353],[7.822126869999998,33.21142578],[7.82466602,33.20541382000001],[7.827038759999999,33.20082855],[7.828558919999999,33.198093410000006],[7.8313098,33.192050930000015],[7.832121849999999,33.18971252000001],[7.83282804,33.18767929],[7.834582809999999,33.188095090000004],[7.837915899999999,33.188892360000004],[7.84586811,33.18852234000001],[7.84986496,33.18844223000001],[7.855095859999999,33.188140870000005],[7.860497,33.18772888],[7.86645508,33.187057499999995],[7.871262070000001,33.18671417],[7.87610722,33.18663025],[7.88125086,33.186508180000004],[7.885625839999999,33.18652725],[7.88996506,33.18628693],[7.89506912,33.185783390000005],[7.898915769999999,33.185565950000004],[7.90223408,33.185295100000005],[7.904187199999999,33.184711459999995],[7.906369210000001,33.18405914],[7.911231990000001,33.18209457],[7.915010929999999,33.180355070000005],[7.919617180000001,33.178676610000004],[7.92373323,33.17684174],[7.927610869999999,33.17563629000001],[7.93249893,33.17335129000001],[7.93739986,33.171600340000005],[7.94221783,33.169597630000005],[7.942699910000001,33.16955566000001],[7.94649076,33.169223790000004],[7.951663970000001,33.16574860000001],[7.95688295,33.163894649999996],[7.96304607,33.16109848],[7.967731949999999,33.159461979999996],[7.972287180000001,33.15803146],[7.97546577,33.156288149999995],[7.97878885,33.15504455999999],[7.98600197,33.153110500000004],[7.99452305,33.14957428],[7.999443050000001,33.14746474999999],[8.00883484,33.14352798],[8.014751429999999,33.1414566],[8.026716230000002,33.13632202000001],[8.032375339999998,33.13433456],[8.037931439999998,33.1320076],[8.04278278,33.129928590000006],[8.048733709999999,33.12874603000001],[8.052037240000002,33.12535858000001],[8.058859830000001,33.124187469999995],[8.06584072,33.12071609000001],[8.07185555,33.11871338000001],[8.07615185,33.11695099000001],[8.080636020000002,33.115448],[8.08569336,33.113796230000005],[8.089378359999998,33.11148834],[8.09977341,33.10702896],[8.10758877,33.10445404],[8.11317921,33.1020813],[8.117963789999997,33.09938049],[8.11790085,33.09601212000001],[8.11780739,33.09106827],[8.117779729999999,33.08577347],[8.11724281,33.08052444],[8.11657715,33.07543945],[8.11620522,33.07079315],[8.115557670000001,33.06647873000001],[8.115009310000001,33.06072617],[8.114993099999998,33.055782320000006],[8.114953039999998,33.05225372],[8.11685658,33.049400330000005],[8.119922639999999,33.046707149999996],[8.124290469999998,33.042442320000006],[8.12839985,33.038852690000006],[8.131826399999998,33.03583527000001],[8.13580418,33.03248978],[8.139531139999999,33.028682710000005],[8.144064899999998,33.02419281000001],[8.148085589999999,33.020671840000006],[8.15221119,33.01721191],[8.156607630000002,33.01405715999999],[8.16020107,33.010635380000004],[8.163286210000003,33.00762939],[8.16966915,33.00453186],[8.229352949999999,32.9430542],[8.247885699999998,32.92396545],[8.28010941,32.88083648999999],[8.32330418,32.823024749999995],[8.324473379999999,32.81206512],[8.326446530000004,32.79355240000001],[8.353167530000002,32.54298782],[8.354744910000003,32.52820587000001],[8.385769840000002,32.53390884000001],[8.62619209,32.59009171],[8.741975779999995,32.61539459],[8.810027119999999,32.6302681],[8.94647789,32.66110992000001],[9.044260029999998,32.662899020000005],[9.062475200000002,32.66323090000001],[9.19566822,32.66453934000001],[9.29048443,32.692329410000006],[9.386866570000002,32.712638850000005],[9.45405674,32.72813416000001],[9.4602747,32.729568480000005],[9.51251316,32.74266815],[9.54889965,32.76293182000001],[9.57263374,32.775901790000006],[9.58752632,32.78356171],[9.60671234,32.787479399999995],[9.62227631,32.78944016],[9.6390276,32.79417038000001],[9.652536390000003,32.799209590000004],[9.663398739999998,32.806278230000004],[9.677178379999996,32.8049202],[9.69379997,32.80257034000001],[9.707527159999998,32.80673981],[9.721472739999998,32.80900954999999],[9.734808919999995,32.81195831],[9.73591805,32.81148147999998],[9.74720669,32.80661011],[9.751378059999997,32.80002213],[9.75501347,32.79428101],[9.7567358,32.78273010000001],[9.76206398,32.773181920000006],[9.767637250000002,32.76319122000001],[9.78117085,32.742820740000006],[9.80412865,32.73155975],[9.82056141,32.72589874000001],[9.83186913,32.72378159],[9.83433628,32.72332001000001],[9.84916019,32.715919490000005],[9.86031628,32.71173859],[9.868099210000002,32.69905853000001],[9.869888310000002,32.695407870000004],[9.875494960000005,32.68395996],[9.88932705,32.67274094000001],[9.90632725,32.67018890000001],[9.91072083,32.66941833000001],[9.923730850000002,32.66712952],[9.93446636,32.664321900000004],[9.945009230000002,32.65927124],[9.9474144,32.65964508],[9.954730030000002,32.66078186],[9.95712185,32.661151890000006],[9.960692410000002,32.671211240000005],[9.959814070000002,32.67226028],[9.952536580000004,32.68096161],[9.940340999999998,32.69065094000001],[9.93015289,32.70175171],[9.91631413,32.713840479999995],[9.893137930000002,32.73012924000001],[9.88799572,32.741291049999994],[9.89300156,32.74553680000001],[9.895435330000003,32.747600559999995],[9.90471363,32.750999449999995],[9.908599850000002,32.75701523000001],[9.911915779999998,32.76214981000001],[9.91810608,32.77362061],[9.918436050000002,32.78364944],[9.91269302,32.79410934000001],[9.91032887,32.80480194],[9.90976238,32.80736923],[9.916187290000002,32.81470871],[9.929311749999998,32.81953049],[9.93532467,32.82500458],[9.93816471,32.82759094000001],[9.94295788,32.83715057],[9.953106880000002,32.842681879999994],[9.95365143,32.84297943],[9.96617699,32.847099299999996],[9.97138786,32.85562897],[9.975382800000002,32.863800049999995],[9.97128677,32.870479579999994],[9.96092224,32.87570953],[9.95892715,32.87654877],[9.94730759,32.881420139999996],[9.94320583,32.888790130000004],[9.942328449999998,32.89426804],[9.94193363,32.896720890000005],[9.942464830000002,32.907611849999995],[9.940773959999998,32.917610169999996],[9.93583107,32.928779600000006],[9.93594456,32.941558840000006],[9.93647957,32.951930999999995],[9.934973719999997,32.9640007],[9.93876934,32.96806717],[9.941820139999995,32.97134018],[9.95092964,32.97351837000001],[9.955664630000005,32.97344971],[9.96370888,32.973331449999996],[9.97668171,32.97452164],[9.99372101,32.97402954],[9.99404812,32.97458267],[9.99733257,32.98012161],[9.993442540000002,32.98595047],[9.986497880000002,32.99311829000001],[9.979787830000005,32.995628360000005],[9.97851181,32.9962616],[9.97266388,32.9991684],[9.966192250000002,33.00242233000001],[9.95923138,33.00592041],[9.94598866,33.013698579999996],[9.93912983,33.015785220000005],[9.93012619,33.018520360000004],[9.919839860000002,33.020538330000015],[9.91854572,33.020790100000006],[9.912586210000004,33.030910490000004],[9.90941143,33.03812408],[9.90722084,33.043098449999995],[9.8986578,33.04800034000001],[9.89384842,33.0520134],[9.89355278,33.052261349999995],[9.893589970000004,33.05242538],[9.89457989,33.05678939999999],[9.896499630000005,33.065250400000004],[9.90108776,33.07635880000001],[9.9108057,33.0802803],[9.92028522,33.0891304],[9.92022133,33.09801102000001],[9.92044544,33.10535812000001],[9.92088032,33.11960983],[9.92173004,33.14258957],[9.92209911,33.17628098],[9.921298979999998,33.20286179],[9.9124794,33.20825958],[9.90414238,33.21335983000001],[9.901371959999995,33.21379471],[9.889887810000005,33.21559906],[9.875037190000002,33.21644974],[9.85877323,33.21537018000001],[9.85132122,33.21464156999999],[9.840484620000002,33.21358109],[9.827220919999998,33.21928024],[9.8100996,33.22320175],[9.795536040000002,33.23768997],[9.78270531,33.239421840000006],[9.77431965,33.24448013],[9.7591753,33.25447083],[9.73762989,33.27404022],[9.711577419999998,33.29560852000001],[9.70484734,33.30070114]]]},\"properties\":{\"CC_1\":null,\"GID_0\":\"TUN\",\"GID_1\":\"TUN.10_1\",\"HASC_1\":\"TN.KB\",\"NAME_0\":\"Tunisia\",\"NAME_1\":\"Kebili\",\"TYPE_1\":\"Wilayat\",\"ENGTYPE_1\":\"Governorate\",\"NL_NAME_1\":null,\"VARNAME_1\":\"Kebilli|Qbili|Qibil\u012b\"}}}"
    }

t = lambda_handler(input_json, 1)