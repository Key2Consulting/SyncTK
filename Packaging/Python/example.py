import synctk

#x = pysynctk.Sync().From(pysynctk.Sour)
#import clr
#clr.AddReference("SyncTK")
#from SyncTK import *

synctk.Sync() \
    .From(synctk.SourceSqlServer("(LocalDb)\MSSQLLocalDB", "SyncTK", "SELECT t.* FROM [dbo].[OddTypes] t")) \
    .Into(synctk.TargetSqlServer("(LocalDb)\MSSQLLocalDB", "SyncTK", "dbo", "DBToDBOddTypes", True)) \
    .Exec()

x = 123