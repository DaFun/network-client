#!/usr/bin/env python

import socket
import select
import re
import sys
from random import shuffle

ids_pattern = re.compile('.*MESG:ids=([\d,]+)and.*')
ip_port_pattern = re.compile('(\d{3})=([\d.]+)@(\d+)')
my_id_pattern = re.compile('.*MESG:registered as (\d{3}).*')
msg_pattern = re.compile('.*MESG:(.*)')
src_pattern = re.compile('SRC:(\d{3}).*')
dst_pattern = re.compile('.*DST:(\d{3}).*')
mnum_pattern = re.compile('.*MNUM:(\d{3}).*')
vl_pattern = re.compile('.*VL:([\d,]+).*')
hct_pattern = re.compile('.*HCT:(\d+).*')

def run_loop():
    mnum = 100
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(0)
    server_address = ('steel.isi.edu', 63682)
    message = 'SRC:000;DST:999;PNUM:1;HCT:1;MNUM:{};VL:;MESG:register'.format(mnum)
    sock.sendto(message, server_address)
    mnum += 1
    watch_for_input = [sys.stdin, sock]
    my_id = ''
    id_to_ip = {}
    while True:
        r, w, x = select.select(watch_for_input, [], [])
        for item in r:
            if item == sock:
                data, addr = sock.recvfrom(1024)
                if 'registered as' in data:
                    my_id = my_id_pattern.match(data).group(1)
                    if my_id:
                        print('Successfully registered. My ID is: {}'.format(my_id))
                elif 'MESG:ids=' in data:
                    id_to_ip = print_ids(data)
                elif 'DST:'+my_id in data and 'PNUM:3' in data:
                    ack(data, addr, sock, 3)
                    #print 'ack'
                elif 'DST:'+my_id in data and 'PNUM:7' in data:
                    ack(data, addr, sock, 7)
                elif 'DST:'+my_id not in data and 'PNUM:3' in data:
                    ready_to_send = ack(data, addr, sock, 3, True, my_id)
                    forward(sock, data, ready_to_send, id_to_ip, my_id)
                    #print(ready_to_send)
                elif 'Error' in data or 'PNUM:0' in data:
                    msg = msg_pattern.match(data).group(1)
                    print('Error Message:')
                    print(msg)
                    #sock.close()
                    #return
            elif item == sys.stdin:
                data = sys.stdin.readline().strip()
                if data == 'ids':
                    message = 'SRC:{};DST:999;PNUM:5;HCT:1;MNUM:{};VL:;MESG:get map'.format(my_id, mnum)
                    sock.sendto(message, server_address)
                    mnum += 1
                elif 'msg' in data:
                    try:
                        _, _dst, _msg = data.split()
                        dst_id = int(_dst)
                        if dst_id < 1 or dst_id > 998:
                            raise ValueError
                        _msg = re.sub('[";:\']', '', _msg)
                        _msg = _msg[:200]
                        if _dst in id_to_ip:
                            dsts = set([_dst])
                            broadcast(sock, dsts, my_id, mnum, _msg, id_to_ip, 3)
                        else:
                            message = 'SRC:{};DST:{};PNUM:{};HCT:9;MNUM:{};VL:{};MESG:{}'.format(my_id, _dst, 3,
                                                                                                mnum, my_id, _msg)
                            dump = 'SRC:{};DST:{};PNUM:3;HCT:9;MNUM:{};VL:;MESG:'.format(my_id, _dst, mnum)
                            forward(sock, dump, message, id_to_ip, my_id)
                        mnum += 1
                    except ValueError:
                        print('Invalid ID or No message is given')
                elif 'all' in data:
                    try:
                        _, _msg = data.split()
                        _msg = re.sub('[";:\']', '', _msg)
                        _msg = _msg[:200]
                        dsts = set(id_to_ip.keys()) - set([my_id])
                        broadcast(sock, dsts, my_id, mnum, _msg, id_to_ip, 7)
                        mnum += 1
                    except ValueError:
                        print('No message is given')
                else:
                    print('Unrecognized command')


def print_ids(data):
    ids = ids_pattern.match(data).group(1)
    ip_ports = ip_port_pattern.findall(data)
    id_to_ip = {}
    print('********************')
    print('Recently Seen Peers:')
    print(ids)
    print('\nKnown addresses:')
    for log in ip_ports:
        print '{}\t\t{}\t{}'.format(log[0], log[1], log[2])
        id_to_ip[log[0]] = (log[1], int(log[2]))
    print('********************')
    return id_to_ip


def ack(data, addr, sock, pnum, forwarding=False, my_id=''):
    msg = msg_pattern.match(data).group(1)
    src = src_pattern.match(data).group(1)
    dst = dst_pattern.match(data).group(1)
    msg_num = mnum_pattern.match(data).group(1)
    if forwarding:
        hct = int(hct_pattern.match(data).group(1))
        vl = vl_pattern.match(data).group(1).split(',')
        if hct < 1:
            print('********************')
            print('Dropped message from {} to {} - hop count exceeded'.format(src, dst))
            print('MESG: {}'.format(msg))
        else:
            if my_id in vl:
                print('********************')
                print('Dropped message from {} to {} - peer revisited'.format(src, dst))
                print('MESG: {}'.format(msg))
            else:
                hct -= 1
                vl.append(my_id)
                ready_to_send = re.sub('HCT:\d+;MNUM:(\d{3});VL:[\d,]+',
                                       'HCT:' + str(hct) + ';MNUM:\\1;VL:' + ','.join(vl), data)
                message = 'SRC:{};DST:{};PNUM:{};HCT:1;MNUM:{};VL:;MESG:ACK'.format(dst, src, pnum + 1, msg_num)
                sock.sendto(message, addr)
                return ready_to_send
    else:
        if pnum == 3:    # message
            print msg
        elif pnum == 7:  # broadcast message
            print('********************')
            print('SRC:{} broadcasted:'.format(src))
            print(msg)
    message = 'SRC:{};DST:{};PNUM:{};HCT:1;MNUM:{};VL:;MESG:ACK'.format(dst, src, pnum+1, msg_num)
    sock.sendto(message, addr)
    return message


def broadcast(sock, dsts, my_id, mnum, msg, id_to_ip, pnum):
    ack_msg = []
    for dst in dsts:
        ack_msg.append('SRC:{};DST:{};PNUM:{};HCT:1;MNUM:{};VL:;MESG:ACK'.format(dst, my_id, pnum+1, mnum))

    for i in range(5):
        for dst in dsts:
            message = 'SRC:{};DST:{};PNUM:{};HCT:1;MNUM:{};VL:;MESG:{}'.format(my_id, dst, pnum, mnum, msg)
            sock.sendto(message, id_to_ip[dst])
        response, _, _ = select.select([sock], [], [], 0.6)
        for res in response:
            while res:
                try:
                    data, addr = sock.recvfrom(1024)
                    if 'DST:' + my_id in data and 'PNUM:3' in data:
                        ack(data, addr, sock, 3)
                    elif data in ack_msg:
                        _dst = src_pattern.match(data).group(1)
                        dsts = set(dsts) - set([_dst])
                        #print "ack"
                except socket.error:
                    break

    for dst in dsts:
        print('********************')
        print('ERROR: Gave up sending to {}'.format(dst))
        print('********************')


def forward(sock, dump, message, id_to_ip, my_id):
    addresses = []
    for id in id_to_ip.keys():
        if id != my_id:
            addresses.append(id_to_ip[id])
    shuffle(addresses)
    peer = addresses[:3]
    src = src_pattern.match(dump).group(1)
    msg_num = mnum_pattern.match(dump).group(1)
    dst = dst_pattern.match(dump).group(1)
    ack_msg = 'SRC:{};DST:{};PNUM:{};HCT:1;MNUM:{};VL:;MESG:ACK'.format(dst, src, 4, msg_num)
    for i in range(5):
        for dst in peer:
            sock.sendto(message, dst)
        response, _, _ = select.select([sock], [], [], 0.6)
        for res in response:
            while res:
                try:
                    data, addr = sock.recvfrom(1024)
                    if data == ack_msg:
                        peer = set(peer) - set([addr])
                except socket.error:
                    break

    for item in peer:
        for dst in id_to_ip.keys():
            if id_to_ip[dst] == item:
                print('********************')
                print('ERROR: Gave up sending to {}'.format(dst))
                print('********************')


if __name__ == '__main__':
    run_loop()