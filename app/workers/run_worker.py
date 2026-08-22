import asyncio
import signal
from app.workers.worker import Worker
from app.core.logging import logger

async def main():
    logger.info("Initializing worker process launcher...")
    worker = Worker()

    # Register OS signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    
    def stop_handler():
        logger.info("Termination signal received. Shutting down worker...")
        worker.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_handler)
        except NotImplementedError:
            # Signal handlers not fully supported on some Windows platforms, fallback
            pass

    try:
        await worker.start()
    except asyncio.CancelledError:
        logger.info("Worker run loop cancelled.")
    except Exception as e:
        logger.error("Fatal error in worker launcher", error=str(e))
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker process exited.")
