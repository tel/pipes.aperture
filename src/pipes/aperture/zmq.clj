(ns pipes.aperture.zmq
  (:refer-clojure :exclude [send])
  (:use [carbonite.api :only [default-registry]]
        [slingshot.slingshot :only [throw+ try+]])
  (:require [carbonite.buffer :as carbon])
  (:import [org.zeromq ZMQ ZMQ$Socket ZMQ$Context]))

;;; Carbonite wrapper
;;; 

(def ^:dynamic *registry* (default-registry))

(defn freeze
  "Use Kyro/Carbonite to write serialize `obj` to a byte[]."
  [obj] (carbon/write-bytes *registry* obj))

(defn thaw
  "Use Kyro/Carbonite to read a byte[] as a Java or Clojure object."
  [bytes] (carbon/read-bytes *registry* bytes))

(defmacro with-serialization-registry
  "Use a custom Kyro/Carbonite registry."
  [registry & body]
  `(binding [*registry* ~registry]
     ~@body))

;;; ZMQ Wrapper
;;; 

;;; TODOs
;;; Currently really doesn't support PUB/SUB since messages are
;;; composed entirely of the serialized payload. To fix this there
;;; needs to be a header of some sort.

;;; Flags
(def patterns
  {:pair       ZMQ/PAIR
   :push       ZMQ/PUSH
   :pull       ZMQ/PULL
   :pub        ZMQ/PUB
   :sub        ZMQ/SUB
   :req        ZMQ/REQ
   :rep        ZMQ/REP
   :xreq       ZMQ/XREQ
   :xrep       ZMQ/XREP})

(def devices
  {nil         0
   :streamer   ZMQ/STREAMER
   :forwarder  ZMQ/FORWARDER
   :queue      ZMQ/QUEUE})

(def send-flags
  {nil         0
   :send-more  ZMQ/SNDMORE
   :no-block   ZMQ/NOBLOCK
   :dont-wait  ZMQ/NOBLOCK})

;;; Context handling
;;;
;;; Creates a default ZMQ context which will be used if no other one
;;; is specified.
(def ^:dynamic *zmqctx* (ZMQ/context 1))

;;; A reload-safe shutdown hook to kill the ZMQ context.
(def *term-hook (atom nil))
(when-not @*term-hook
  (let [term-thread (new Thread (fn [] (.term *zmqctx*)))]
    (swap! *term-hook (constantly term-thread))
    (when (identical? @*term-hook term-thread)
      (.. Runtime (getRuntime) (addShutdownHook term-thread)))))

;;; ZMQ Ops
;;; 
(defn socket
  "Returns a socket in the current (or default) ZMQ context which
  satisfies the pattern description (or socket flag) `pattern`. Global
  lookup is performed from the `patterns` variable. Defaults to
  a :pair socket.

  Options include
  :close-timeout --- How long after close/term will the socket linger
                     before dying?
  "
  ([] (socket :pair))
  ([pattern-flag]
     (socket pattern-flag {}))
  ([pattern-flag options]
     (let [sock (.socket *zmqctx* (if (integer? pattern-flag)
                                    pattern-flag
                                    (patterns pattern-flag)))]
       ;; Do the options
       
       ;; set LINGER
       (when (:close-timeout options)
         (.setLinger sock (:close-timeout options))))))

(def *protocol-prefixes
  {:inproc "inproc://"
   :ipc    "ipc://"
   :tcp    "tcp://"
   :pgm    "pgm://"
   :epgm   "epgm://"})

(defn- parse-address [x]
  (cond
   ;; It's just a regular ZMQ connection string
   (string? x) x
   ;; It's of the form [type location]
   (sequential? x) (let [[type location] x]
                     (str (get *protocol-prefixes type) (str location)))
   :else (throw+ {:fatal "Unknown ZMQ protocol"
                  :protocol x})))

(defn bind
  "Binds a socket to listen on a particular address"
  [^ZMQ$Socket sock address] (.bind sock (parse-address address)))

(defn connect
  "Connects a socket to send to a particular address"
  [^ZMQ$Socket sock address] (.connect sock (parse-address address)))

(defn close
  "Closes a socket."
  [^ZMQ$Socket sock] (.close sock))


(defmacro with-sockets
  "Create and connect/bind a series of sockets in a `with-close` block.

   Socket specs are
   [[(:bind/:connect) var pattern-flags address & options]
    ...]

   See the doc for `socket` for options. If you want to do multiple
   binds or connects then it's probably better to use `with-close`
   manually. This is best for the common case of single connections
   and binds."
  [socket-specs & body]
  (let [[[type var pattern-flag address & options] & rest] socket-specs]
    (if (and type var pattern-flag address)
      `(with-open [~var (doto (socket ~pattern-flag ~options)
                          ~(cond
                            (= type :bind)    `(bind ~address)
                            (= type :connect) `(connect ~address)
                            :else
                            (throw+ {:fatal "with-sockets only accepts :bind and :connect types"
                                     :type type})))]
         (with-sockets ~rest ~@body))
      `(do ~@body))))

(defn send
  "Send an object through a socket. Flags can be appended optionally."
  ([^ZMQ$Socket sock obj flags]
     (.send sock (freeze obj)
            (if (integer? flags)
              flags
              (send-flags flags))))
  ([^ZMQ$Socket sock obj] (send sock obj (send-flags nil))))

(defn recv
  "Recieve an object through a socket. Flags can be appended optionally."
  ([^ZMQ$Socket socket flags]
     (when-let [bytes (.recv socket
                             (if (integer? flags)
                               flags
                               (send-flags flags)))]
       (thaw bytes)))
  ([^ZMQ$Socket socket] (recv socket (send-flags nil))))