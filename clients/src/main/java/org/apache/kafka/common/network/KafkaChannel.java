/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.network;


import java.io.IOException;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.utils.Utils;

public class KafkaChannel {
    private final String id;
    private final TransportLayer transportLayer;//传输层,封装了SocketChannel,很多实际要操作SocketChannel时,都交给传输层处理
    private final Authenticator authenticator;
    private final int maxReceiveSize;
    private NetworkReceive receive;//接收的响应
    private Send send;//发送的请求
    // Track connection and mute state of channels to enable outstanding requests on channels to be
    // processed after the channel is disconnected.
    private boolean disconnected;
    private boolean muted;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
        this.disconnected = false;
        this.muted = false;
    }

    public void close() throws IOException {
        this.disconnected = true;
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator
     */
    public void prepare() throws IOException {
        if (!transportLayer.ready())
            transportLayer.handshake();
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        disconnected = true;
        transportLayer.disconnect();
    }


    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public void mute() {
        if (!disconnected)
            transportLayer.removeInterestOps(SelectionKey.OP_READ);
        muted = true;
    }

    public void unmute() {
        if (!disconnected)
            transportLayer.addInterestOps(SelectionKey.OP_READ);
        muted = false;
    }

    /**
     * Returns true if this channel has been explicitly muted using {@link KafkaChannel#mute()}
     */
    public boolean isMute() {
        return muted;
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    public void setSend(Send send) {
        //如果Kafka通道上还有未发送成功的Send请求，则后面的新来的请求就不能发送
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        this.send = send; //没有未发送成功的请求,则将当前请求放入当前对象的send成员属性
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);//向Selector加入写事件
    }

    //Selector轮询时，检测到读事件时调用此方法
    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        //如果网络接收对象不存在，则新建一个
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }
        //
        receive(receive);
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        return result;
    }

    //Selector轮询的时候,检测到写时间就会调用这个方法
    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            result = send;
            //请求已经全部发送完成 ， 重置send对象为空 ，下一次新的请求才可以继续正常进行
            send = null;
        }
        return result;
    }

    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    private boolean send(Send send) throws IOException {
        send.writeTo(transportLayer);
        if (send.completed())
            //Send发送成功后，就要取消写事件
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        return send.completed();//Send没有写完成，会监听写事件
    }

}
