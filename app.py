"""Local-first dashboard for the CDC workers."""

import argparse
import atexit
import logging
import threading

import pymysql
from flask import Flask, jsonify, render_template, request

from cdc import CDCError, Manager, load_config


def create_app(manager):
    app = Flask(__name__)
    control_lock = threading.Lock()

    @app.before_request
    def validate_control_request():
        if request.method == "POST" and (
            request.headers.get("X-CDC-Action") != "1" or not request.is_json
        ):
            return jsonify(error="A JSON request with X-CDC-Action: 1 is required."), 403

    @app.after_request
    def protect(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/status")
    def status():
        return jsonify(manager.status())

    @app.post("/api/cutover/capture")
    def capture_cutover():
        body = request.get_json()
        if not isinstance(body, dict) or body.get("confirm") != "CAPTURE SOURCE BARRIER":
            return jsonify(error="Confirm CAPTURE SOURCE BARRIER."), 400
        try:
            with control_lock:
                captured = manager.capture_cutover()
        except (pymysql.MySQLError, CDCError, OSError) as error:
            app.logger.exception("Cannot capture source cutover position")
            return jsonify(error=f"Cannot capture source position: {error}"), 503
        return jsonify(
            message="Source barrier captured. Monitor all configured tables; primary-to-replica readiness is not verified.",
            cutover=captured,
        ), 201

    @app.post("/api/tables/<table>/<action>")
    def action(table, action):
        worker = manager.workers.get(table)
        if not worker:
            return jsonify(error="Unknown source table."), 404
        try:
            with control_lock:
                if action == "pause":
                    worker.pause()
                elif action == "resume":
                    worker.start()
                elif action == "initialize":
                    body = request.get_json()
                    if not isinstance(body, dict) or body.get("confirm") != table:
                        return jsonify(error="Confirm the source table name."), 400
                    worker.start(initialize=True)
                else:
                    return jsonify(error="Unknown action."), 400
        except CDCError as error:
            return jsonify(error=str(error)), 409
        return jsonify(message="Request accepted. Check the table status."), 202

    @app.post("/api/all/<action>")
    def all_tables(action):
        with control_lock:
            if action == "pause":
                for worker in manager.workers.values():
                    worker.pause()
            elif action == "initialize":
                body = request.get_json()
                if not isinstance(body, dict) or body.get("confirm") != "REINITIALIZE ALL":
                    return jsonify(error="Confirm REINITIALIZE ALL."), 400
                if any(w.thread and w.thread.is_alive() for w in manager.workers.values()):
                    return jsonify(error="Pause all tables and wait until they stop first."), 409
                for worker in manager.workers.values():
                    worker.start(initialize=True)
            else:
                return jsonify(error="Unknown action."), 400
        return jsonify(message="Request accepted. Check every table status."), 202

    return app


def main():
    parser = argparse.ArgumentParser(description="MySQL CDC dashboard")
    parser.add_argument("--config", default="config.yaml", help="YAML configuration file")
    arguments = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    manager = Manager(load_config(arguments.config))
    app = create_app(manager)
    manager.initialize_store()
    atexit.register(manager.close)
    try:
        app.run(
            host=manager.config["web"]["host"], port=manager.config["web"]["port"],
            debug=False, use_reloader=False,
        )
    finally:
        manager.close()


if __name__ == "__main__":
    main()
