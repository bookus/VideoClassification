
rm -rf /workspace/data/video/videos/temp*

ps -ef|grep pipline|grep -v grep|cut -c 9-15|xargs kill -9

ps -ef|grep export_frames|grep -v grep|cut -c 9-15|xargs kill -9
