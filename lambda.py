from __future__ import print_function

import json
import urllib
import boto3
import math

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    response = s3.get_object(Bucket=bucket, Key=key)
    #get the body of the file as a string
    data = response['Body'].read()

    filtered_angle_list=[]
    #calculate the angle
    angle_dict=calculate_angle(data)
    #filter the results
    filtered_pitch_angle_list = FIR_filter(angle_dict['pitch_angle'])
    filtered_roll_angle_list = FIR_filter(angle_dict['roll_angle'])

    #store the angles one after the other (pitch0,roll0,pitch1,roll1,pitch2,roll2...)    
    list_min=min(len(filtered_pitch_angle_list),len(filtered_roll_angle_list))
    for i in range(list_min):
        filtered_angle_list.append(filtered_pitch_angle_list[i])
        filtered_angle_list.append(filtered_roll_angle_list[i])
        
    #filtered_angle_list = filtered_pitch_angle_list+filtered_roll_angle_list

    filtered_data = "\n".join(filtered_angle_list)    
    new_file_key=key.split(".")[0]+"_processed.txt"
    if not ("processed") in key:
        s3.put_object(Bucket=bucket, Key=new_file_key,Body=filtered_data)
    return filtered_data

#parameter: a list of measurements (ax0,ay0,az0,ax1,ay1,az1,ax2,ay2,az2...)
#return: a list of angles
def calculate_angle(data):
    pitch_angle_list=[]
    roll_angle_list=[]
    ax_list=[]
    ay_list=[]
    az_list=[]
    data_list=data.split()
    angle_dict={}
    
    for i in range(len(data_list)):
        #print (str(i)+" "+data_list[i])
        measurement= int(data_list[i])
        #if the element position has a modulo of 3==0 , it is a ax value
        if i%3==0:
            ax_list.append(measurement)
        #if the element position has a modulo of 3==1, it is a ay value
        elif i%3==1:
            ay_list.append(measurement)
        #if the element position has a modulo of 3==2, it is a ay value
        elif i%3==2:
            az_list.append(measurement)
            
    #get the list with the minimum number of elements to avoid out of bound error
    list_min=min(len(ax_list),len(ay_list),len(az_list))
    
    #calculate the pitch and roll angle
    for i in range(list_min):
        pitch_angle = 57.295*math.atan(ay_list[i]/(math.sqrt((ax_list[i]*ax_list[i])+(az_list[i]*az_list[i]))))
        pitch_angle_list.append(pitch_angle)    
        roll_angle = 57.295*math.atan(ax_list[i]/(math.sqrt((ay_list[i]*ay_list[i])+(az_list[i]*az_list[i]))))
        roll_angle_list.append(roll_angle) 
    
    angle_dict['pitch_angle']=pitch_angle_list
    angle_dict['roll_angle']=roll_angle_list        
    return angle_dict

#parameter: a list of unfiltered data
#return: a list of filtered data    
def FIR_filter(data):
    filtered_data_list=[]
    coeff=[0.06136,0.24477,0.38774,0.24477,0.06136]
    
    for n in range(len(data)-len(coeff)-1):
        sum = 0
        for b in range(len(coeff)-1):
            sum += data[n+b]*coeff[b]
	    filtered_data_list.append(str(int(sum)))


    return filtered_data_list