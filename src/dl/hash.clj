(ns dl.hash)

(import '(java.util.zip CRC32))
(import '(org.apache.commons.codec.digest DigestUtils))

(defn crc32 [x]
  (format "%08x" (.getValue (doto (CRC32.) (.update x)))))

(defn multi [x]
  {
    :crc32 (future (crc32 x))
    :l (future (count x))
    :md5 (future (DigestUtils/md5Hex x))
    :sha1 (future (DigestUtils/sha1Hex x))
    :sha2_256 (future (DigestUtils/sha256Hex x))
  }
)
