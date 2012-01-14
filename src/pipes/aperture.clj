(ns pipes.aperture
  (:use [pipes.core]
        [slingshot.slingshot :only [throw+ try+]])
  (:require [pipes.aperture.zmq :as mq]))

