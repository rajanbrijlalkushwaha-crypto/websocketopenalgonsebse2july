#!/bin/bash
# Stop all pipeline processes
pkill -f "pipeline.socketio_server"  && echo "Socket.io stopped"
pkill -f "pipeline.scheduler"         && echo "Scheduler stopped"
pkill -f "pipeline.producer"          && echo "Producer stopped"
pkill -f "pipeline.consumers"         && echo "Consumers stopped"
echo "All pipeline processes stopped."
