import aimts3ops as aws
import aimtfileops as flops

awsservicename = 's3'
profile = 'aim_rds_s3user'
aimbucket = 'aimdhs3'
s3path = 'QA/RAPP/Outbound/'
flpat = 'AIM_Roster'
tgts3path = 'QA/RAPP/Outbound/HCP_Master/'

s3conn = aws.Aimts3ops(profile, awsservicename)

uploadlst = flops.Aimtfileops.filelistindir('C:\\Users\GKrishnamoorthy\OneDrive - Aimmune Therapeutics, Inc\Documents\Aimmune\\2020\MISC\For UBC',
                                            'AIM_')
print(uploadlst)

for itm in range(len(uploadlst)):
    uploadlst[itm] = "C:\\Users\GKrishnamoorthy\OneDrive - Aimmune Therapeutics, Inc\Documents\Aimmune\\2020\MISC\For UBC\\" + uploadlst[itm]

print(uploadlst)

s3conn.upldtos3fromlocal(aimbucket, uploadlst, s3path)
#s3conn.removefiles3(aimbucket, objlist)

objlist = s3conn.lists3objects(aimbucket, s3path + flpat)

for obj in objlist:
    print(obj)

s3conn.copys3tos3(aimbucket, aimbucket, objlist, tgts3path)
