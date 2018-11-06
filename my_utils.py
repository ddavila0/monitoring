import pdb

# Convert HTCondor ClassAd list to json
def convert_ClassAd_to_json(ad):
  #pdb.set_trace()
  # split ad into lines 
  thestrings=str(ad).splitlines()
  json_ad=""
  for thestring in thestrings :
    # remove initial and trailing whitespace
    newstring1=thestring.strip()
    newstring2=newstring1.replace("]","},")
    newstring3=newstring2.replace("[","{")
    newstring4=newstring3.replace(";",",")
    # replace only the first occurance of "=" as x509 Ads have it in the value
    newstring5=newstring4.replace("=","::",1)
    if len(newstring5) > 0 :
      try :
        adname=newstring5.split("::")[0].strip()
        value=newstring5.split("::")[1].strip()
        # record if trailing comma on line
        if value[-1] == "," :
          thecomma=","
        else :
          thecomma=""
        # remove from value final comma, quotes, illegal control chars
        tmpvalue=value.rstrip(",").replace("\"","").replace("\'","")
        newvalue=tmpvalue.replace("\\","").replace("\\","")
        try :
          float(newvalue)
          json_ad +="  \""+adname+"\" : "+newvalue+thecomma
        except ValueError :
          # if not a number, then encase value in double quotes
          json_ad += "  \""+adname+"\" : \""+newvalue+"\""+thecomma
      except IndexError :
        json_ad += newstring5

  return json_ad
