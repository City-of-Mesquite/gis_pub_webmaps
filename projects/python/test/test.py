
from pymssql import _mssql
import arcpy
from os import getenv

server = getenv("GISDS")
user = getenv("GIS_USER")
password = getenv("gEfOpu5LNVvA9Hd")
arcpy.env.workspace = r"Q:\GIS\Projects\SDE_CONNECTION_FILES\GISDS_GIS_TEST.sde"



#disconnect
print "Begin disconnecting all users"
try:
    arcpy.DisconnectUser("Q:\GIS\Projects\SDE_CONNECTION_FILES\GISDS_GIS_TEST.sde", "ALL")
    print "users disconnected"
except:
    print str(Exception.message)


#reconcile and post edits in all versions
print "being reconciling, posting & deleting all versions but default and replication version"

versionList = arcpy.ListVersions(r"Q:\GIS\Projects\SDE_CONNECTION_FILES\GISDS_GIS_TEST.sde")
for version in versionList:
    if (version != 'dbo.DEFAULT'):

        try:
            arcpy.ReconcileVersions_management(input_database="Q:\GIS\Projects\SDE_CONNECTION_FILES\GISDS_GIS_TEST.sde", reconcile_mode="ALL_VERSIONS", target_version="dbo.DEFAULT",edit_versions = version, acquire_locks="LOCK_ACQUIRED", abort_if_conflicts="NO_ABORT", conflict_definition="BY_OBJECT", conflict_resolution="FAVOR_TARGET_VERSION", with_post="POST", with_delete="DELETE_VERSION")
            print "R&P done for: " + version
        except ValueError, Argument:
            print "there is an error: ", ValueError.message
            break

#Compress
print "begin Analyze & Compress"
try:
    arcpy.Analyze_management(in_dataset="gis_test.DBO.Eng_Water",components="BUSINESS;FEATURE;ADDS;DELETES")
    print 'analyze water'
except:
    print str(Exception.message)
try:
    arcpy.Analyze_management(in_dataset="gis_test.DBO.Eng_Sewer",components="BUSINESS;FEATURE;ADDS;DELETES")
    print 'analyze sewer'
except:
    print str(Exception.message)
try:
    arcpy.Analyze_management(in_dataset="gis_test.DBO.Eng_Drainage",components="BUSINESS;FEATURE;ADDS;DELETES")
    print 'analyze drainage'
except:
    print str(Exception.message)

print   "now I will compress"
#compress
try:
    arcpy.Compress_management(r"Q:\GIS\Projects\SDE_CONNECTION_FILES\GISDS_GIS_TEST.sde")
    print 'compressed GISDS'
except:
    print str(Exception.message)

print "analyze again after compress"
try:
    arcpy.Analyze_management(in_dataset="gis_test.DBO.Eng_Water",components="BUSINESS;FEATURE;ADDS;DELETES")
    print 'analyze 2 water '
except:
    print str(Exception.message)
try:
    arcpy.Analyze_management(in_dataset="gis_test.DBO.Eng_Sewer",components="BUSINESS;FEATURE;ADDS;DELETES")
    print 'analyze 2 sewer'
except:
    print str(Exception.message)
try:
    arcpy.Analyze_management(in_dataset="gis_test.DBO.Eng_Drainage",components="BUSINESS;FEATURE;ADDS;DELETES")
    print 'analyze 2 drainage'
except:
    print str(Exception.message)


#
#print "Begin stored procedure"
#msSqlConn = _mssql.connect(server=r'gisds',user=r'gis_user',password=r'gEfOpu5LNVvA9Hd',database=r'gis_test')
#
#
#print "Begin Water"
#WaterfcList = arcpy.ListFeatureClasses("*", "", feature_dataset="GIS_TEST.DBO.Eng_Water")
#for fc in WaterfcList:
#    if fc == "GIS_TEST.DBO.WMAIN":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[WMAIN]', 'WM'))
#            print "wmain done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#    elif fc == "GIS_TEST.DBO.WVALVE":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[WVALVE]', 'WS'))
#            print "WVALVE done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#    elif fc == "GIS_TEST.DBO.WPIPEFIT":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[WPIPEFIT]', 'WP'))
#            print "WPIPEFIT done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#    elif fc == "GIS_TEST.DBO.WSERVICE":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[WSERVICE]', 'WS'))
#            print "WSERVICE done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#    elif fc == "GIS_TEST.DBO.WFIREHYD":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[WFIREHYD]', 'FH'))
#            print "WFIREHYD done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#    elif fc == "GIS_TEST.DBO.WNODE":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[WNODE]', 'WN'))
#            print "WNODE done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#    elif fc == "GIS_TEST.DBO.W_ABANDONED":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[W_Abandoned]', 'WA'))
#            print "W_Abandoned done"
#        except ValueError, Argument:
#            print "there is an error", argument
#    elif fc == "GIS_TEST.DBO.ENG_WATER_NETWORK_JUNCTIONS":
#        pass
#
#
#print 'Water is done'
#
##BEGIN SEWER
#print 'Begin on sewer'
#SewerfcList = arcpy.ListFeatureClasses("*", "", feature_dataset="gis_test.DBO.Eng_Sewer")
#
#for fc in SewerfcList:
#    if fc == "GIS_TEST.DBO.ABANDONED_MANHOLES":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[ABANDONED_MANHOLES]', 'SH'))
#            print "ABANDONED_MANHOLES done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "GIS_TEST.DBO.ABANDONED_SEWER":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[ABANDONED_SEWER]', 'SA'))
#            print "ABANDONED_SEWER done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "GIS_TEST.DBO.SPIPEFIT":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[SPIPEFIT]', 'SP'))
#            print "SPIPEFIT done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "GIS_TEST.DBO.SNODE":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[SNODE]', 'SN'))
#            print "SNODE done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "GIS_TEST.DBO.SMANHOLE":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[SMANHOLE]', 'SM'))
#            print "SMANHOLE done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "GIS_TEST.DBO.SLIFTSTA":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[SLIFTSTA]', 'SL'))
#            print "SLIFTSTA"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "GIS_TEST.DBO.SGMAIN":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[SGMAIN]', 'SG'))
#            print "SGMAIN done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "GIS_TEST.DBO.SCLNOUT":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[SCLNOUT]', 'SC'))
#            print "SCLNOUT done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "GIS_TEST.DBO.AERIAL_CROSSINGS":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[AERIAL_CROSSINGS]', 'AC'))
#            print "AERIAL_CROSSINGS done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#    elif fc == "GIS_TEST.DBO.Eng_Sewer_Network_Junctions":
#        pass
#
#print "sewer is done"
#DrainagefcList = arcpy.ListFeatureClasses("", "", feature_dataset="gis_test.DBO.Eng_Drainage")
#for fc in DrainagefcList:
#    if fc == "gis_test.DBO.Drainage_Area_Maps":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[Drainage_Area_Maps]', 'DM'))
#
#            print "Drainage_Area_Maps done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "gis_test.DBO.DPIPES":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[DPIPES]', 'DP'))
#            print "DPIPES done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "gis_test.DBO.DPIPEFIT":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[DPIPEFIT]', 'DF'))
#
#            print "DPIPEFIT done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "gis_test.DBO.DNODE":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[DNODE]', 'DN'))
#            print "DNODE done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "gis_test.DBO.DINLET":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[DINLET]', 'DI'))
#            print "DINLET done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "gis_test.DBO.Detention_Ponds":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[Detention_Ponds]', 'DT'))
#            print "Detention_Ponds done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "gis_test.DBO.DENDTRMT":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[DENDTRMT]', 'DE'))
#            print "DENDTRMT done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#    elif fc == "gis_test.DBO.DCHANNEL":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[DCHANNEL]', 'DC'))
#            print "DCHANNEL done"
#        except ValueError, Argument:
#            print "there is an error ", Argument
#
#    elif fc == "gis_test.DBO.DBOXCUL":
#        try:
#            sqlcmd = """
#                dbo.SP_UTILITY_MUNISID_DUPES %s, %s
#                """
#            res = msSqlConn.execute_non_query(sqlcmd, ('[DBO].[DBOXCUL]', 'BC'))
#            print "DBOXCUL done"
#        except ValueError, Argument:
#            print "there is an error", Argument
#
#msSqlConn.close()


print "Rebuild Spatial Indexes"
try:
    fcList = arcpy.ListFeatureClasses()
    for fc in fcList:
        print "rebuilding spatial index on " + fc
        arcpy.AddSpatialIndex_management(fc,0,0,0)

except:
    print "index on " + fc + " went horribly wrong..."
    print str(Exception.message)



# #synchronize changes ONE
print 'Begin Synchronize - ONE'
try:
    arcpy.SynchronizeChanges_management(geodatabase_1="Q:\GIS\Projects\SDE_CONNECTION_FILES\GISDS_GIS_TEST.sde", in_replica="DBO.GIS_TEST_Replica", geodatabase_2= r"Q:/GIS/Admin/zJourney/Replication/UTILITY.gdb", in_direction="FROM_GEODATABASE1_TO_2", conflict_policy="MANUAL", conflict_definition="BY_OBJECT", reconcile="DO_NOT_RECONCILE")
    print 'synchronized to relative replica is done - ONE'
except:
    print str(Exception.message)

# #synchronize changes
print 'Begin Synchronize - TWO'
try:
    arcpy.SynchronizeChanges_management(geodatabase_1="Q:\GIS\Projects\SDE_CONNECTION_FILES\GISDS_GIS_TEST.sde", in_replica="DBO.GIS_TEST_Replica", geodatabase_2= r"Q:/GIS/Admin/zJourney/Replication/UTILITY.gdb", in_direction="FROM_GEODATABASE1_TO_2", conflict_policy="MANUAL", conflict_definition="BY_OBJECT", reconcile="DO_NOT_RECONCILE")
    print 'synchronized to relative replica is done'
except:
    print str(Exception.message)


print 'ALL DONE!'




