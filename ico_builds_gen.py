#!/bin/env python2.7

#Python version 2.7.5
from datetime import datetime
import requests, json
import sys, csv, re
import os
import netrc, paramiko

config = load_config('builds_gen.conf')
cmrm_url = config['cmrm_url']
ico_server = config['ico_server']
n = netrc.netrc()
auth = n.authenticators(ico_server)
usr = auth[0]
passwd = auth[2]
rem_path = '/tmp/'
logs_loc = os.path.join(os.path.expanduser('~'), 'ico_logs')
min_log_size = 5.0

filename = "ICO_builds_{}.csv".format('{:%Y-%m-%d %H%M}'.format(datetime.today()))
headers_full = config['headers_full']
headers = config['headers']

def load_config(file):
	with open(file, 'r') as f:
		return json.load(f)

def send_request(ch_nr):
	#API request for change details
	data = {	"data" : { "change_number" : "{}".format(ch_nr),
					"instance" : "X",
					"customer" : "X"
	 					}
			}
	data=json.dumps(data)
	rest_headers = {'Content-type': 'application/json'}

	response = requests.post(cmrm_url, data=data, headers=rest_headers, verify=False)
	print "CMRM Response code {}".format(response.status_code)
	if not response.status_code == 200:	exit(1)
	response = response.json()[0]
	return response
# response = requests.post(cmrm_url, data=json.dumps(data), headers=headers, verify=False, auth=(usr,passwd))

def format_date(dates_list):
	start_date = datetime.strptime(min(dates_list), '%Y-%m-%d %H:%M:%S.%f').replace(microsecond=0)
	end_date = datetime.strptime(max(dates_list), '%Y-%m-%d %H:%M:%S.%f').replace(microsecond=0)
	date_diff = abs(end_date - start_date)
	dur = get_duration(date_diff)
	formatted =	{	"start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
				"end_date" : end_date.strftime("%Y-%m-%d %H:%M:%S"),
				"date_diff" : date_diff,
				"duration" : dur }
	# print formatted
	return formatted

def get_duration(tdelta):
	hours, remainder = divmod(tdelta.total_seconds(), 60*60)
	minutes, seconds = divmod(remainder, 60)
	x = "{}h {}min".format(int(hours), int(minutes))
	return x

def parse_ch(resp_data, *args):
	#Get data about vm builds
	dates = []
	ci_data = None
	for log_m in resp_data['data']['worklog']:
		dates.append(log_m['date'])
		if re.search('inputs from consumer', log_m['description']):
			cust_inputs_str = log_m['long_description'].split('\n')[1]
			cust_inputs = json.loads(cust_inputs_str)
		elif re.search('ci_data', log_m['description']):
			ci_data = log_m['long_description'].split(':', 1)[1]
			ci_data = json.loads(ci_data)

	command = cust_inputs['command']
	print "COMMAND: {}".format(command)
	
	if re.match('viDeploy', command):
		d = format_date(dates)
		row_dict = {key:"n/a" for key in headers}
		row_dict['date']=datetime.strptime(resp_data['data']['target_start'], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')
		row_dict['requester']='x x User' if re.search('x', cust_inputs['source']) else 'ICO User'
		row_dict['reqstr_name']=cust_inputs['localRequestor']
		row_dict['desc']=cust_inputs['arguments']['vi']['patternName']
		row_dict['req_id']=cust_inputs['localRequestId']
		row_dict['ico_id']=args[0] if args[0] else "n/a"
		row_dict['vm_name']=ci_data['[Q_AHOSTNAME]'] if ci_data else "n/a"
		row_dict['ch_number']=resp_data['data']['change_number']
		row_dict['start_date']=d['start_date']
		row_dict['end_date']=d['end_date']
		row_dict['req_json']=cust_inputs_str if cust_inputs_str else "n/a"
		row_dict['global_result']='Completed' if resp_data['data']['status']=='COMP' else 'Failed'
		row_dict['duration']=d['duration']
		print row_dict
		return row_dict
	else: return {}

def get_remote_files(usr, pswd, rem_path, size=min_log_size):
	# Download all relevant log files
	ssh = paramiko.SSHClient()
	ssh.load_system_host_keys()
	ssh.connect(ico_server, username=usr, password=pswd)

	sftp = ssh.open_sftp()
	sftp.chdir(rem_path)

	file_list = []
	for i in sftp.listdir_iter():
		if re.search('VMs-API-Call', i.filename) and i.st_size /float(1<<20) > size:
			file_list.append(os.path.join(sftp.getcwd(), i.filename).replace("\\","/"))

	# print file_list
	for f in file_list:
		local_file = os.path.join(logs_loc, os.path.basename(f))
		if not os.path.exists(local_file):
			print "Downloading log file: {}  from server  {}".format(f, ico_server)
			sftp.get(f, local_file)

	sftp.close()
	ssh.close()

def get_changes(scan_dir):
	#Extract all change numbers from the log files
	changes = {}
	f_list = os.listdir(scan_dir)
	for file in f_list:
		with open(os.path.join(scan_dir, file)) as f:
			for line in reversed(f.readlines()):
				dpl = re.search(r"^(?=.*\boperationContextString\b)(?=.*\bviDeploy\b).*$",line, re.M)
				if dpl:
					chg = re.search(r"\"icdChangeId\\\":\\\"CH[0-9]*\\\"",line, flags=re.M)
					reqid = re.search(r"\"icoRequestId\\\":\\\"([0-9]*)\\\"",line)

					if chg and reqid:
						print "CHG  {}   REQID   {}".format(chg.group(0), reqid.group(0))
						x = [x.replace('\\"','').split(':')[1] for x in (reqid.group(0), chg.group(0))]
						changes.update({x[1]: x[0]})
					break
				else:
					continue
	return changes


#======MAIN============
if not os.path.exists(logs_loc):
	print "Creating dir: {}".format(logs_loc)
	os.mkdir(logs_loc)

get_remote_files(usr, passwd, rem_path)
changes_d = get_changes(logs_loc)
print changes_d

with open(filename, 'wb') as csvfile:
	head_writer = csv.DictWriter(csvfile, fieldnames=headers_full)
	head_writer.writeheader()
	writer = csv.DictWriter(csvfile, fieldnames=headers)
	for ch, reqid in changes_d.iteritems():
		cmrm_data = send_request(ch)
		csv_row = parse_ch(cmrm_data, reqid)
		writer.writerow(csv_row)
	print "Results stored in file {}".format(csvfile.name)


