import ijson, pycurl

filename = "obfuscated_data"
f_ile = open(filename,'rb')
product_list = {}
device_list = {}
flag = 0
event_dict = {}
index1 = 0
result_file = "analysis.txt"

STREAM_URL = "http://ew1-fscdev-ds-public.s3-website-eu-west-1.amazonaws.com/obfuscated_data.xz"
'''
class Client:
  def __init__(self):
    self.buffer = ""
    self.conn = pycurl.Curl()
    self.conn.setopt(pycurl.USERPWD, "%s:%s" % (USER, PASS))
    self.conn.setopt(pycurl.URL, STREAM_URL)
    self.conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)
    self.conn.perform()

  def on_receive(self, data):
    self.buffer += data
    if data.endswith("rn") and self.buffer.strip():
      content = json.loads(self.buffer)
      self.buffer = ""
      print content

client = Client()
'''
def main():

	file_target = open(result_file,"w")
	for prefix,event,value in parser:
		if (prefix, event) == ("item", "start_map"):
			event_dict[index1] = {}
		elif (prefix) == ("item.type") and value in ["user_interaction","crash","metric","purchase"]:
			flag = 1
		elif (prefix,event) == ("item.device.device_id","string"):
			event_dict[index1]['device_id'] = value
		elif (prefix,event) == ("item.application.name","string"):
			event_dict[index1]['application_name'] = value
		elif (prefix,event) == ("item.user_interaction.view_id","string"):
			event_dict[index1]['application_view_id'] = value
		elif (prefix,event) == ("item.device.model","string"):
			event_dict[index1]['model'] = value
		elif (prefix,event) == ("item.sender_info.geo.country","string"):
			event_dict[index1]['city'] = value
		elif prefix == 'item.network.carrier':
			event_dict[index1]['carrier_network'] = value
		elif (prefix,event) == ("item.purchase.product_id","string"):
			event_dict[index1]['purchase_product_id'] = value
		elif prefix == 'item.purchase.amount':
			event_dict[index1]['amount'] = float(value)
		elif (prefix,event) == ("item.purchase.type","string"):
			event_dict[index1]['type'] = value
		elif (prefix,event) == ("item.purchase.stage","string"):
			event_dict[index1]['stage'] = value
		elif (prefix,event) == ("item.purchase.currency","string"):
			event_dict[index1]['currency'] = value
		elif (prefix,event) == ("item.source","string"):
			event_dict[index1]['source'] = value
		elif (prefix,event) == ("item.error.message","string"):
			event_dict[index1]['error_message'] = value.replace(" ","")
		elif (prefix,event) == ("item.error.reason","string"):
			event_dict[index1]['error_reason'] = value.replace(" ","")
		elif (prefix, event) == ("item", "end_map"):
			if flag == 0:
				del event_dict[index1]
			else:
				flag = 0
			if index1 in event_dict.keys():
				for ind in event_dict[index1]:
					try:
						file_target.write(str(event_dict[index1][ind]) + '\t')
					except:
						file_target.write(" " + '\t')
				file_target.write("\n")
			index1 = index1 + 1
	
	file_target.close()

if __name__=='__main__':
	main()
