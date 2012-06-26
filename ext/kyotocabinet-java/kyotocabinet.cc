/*************************************************************************************************
 * Java binding of Kyoto Cabinet.
 *                                                               Copyright (C) 2009-2011 FAL Labs
 * This file is part of Kyoto Cabinet.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/


#include <kcpolydb.h>
#include <kcdbext.h>
#include <jni.h>

#include "kyotocabinet_Utility.h"
#include "kyotocabinet_Error.h"
#include "kyotocabinet_Cursor.h"
#include "kyotocabinet_DB.h"
#include "kyotocabinet_MapReduce.h"
#include "kyotocabinet_ValueIterator.h"

#define P_OBJ     "java/lang/Object"
#define L_OBJ     "L" P_OBJ ";"
#define P_STR     "java/lang/String"
#define L_STR     "L" P_STR ";"
#define P_ERR     "kyotocabinet/Error"
#define L_ERR     "L" P_ERR ";"
#define P_VIS     "kyotocabinet/Visitor"
#define L_VIS     "L" P_VIS ";"
#define P_FPROC   "kyotocabinet/FileProcessor"
#define L_FPROC   "L" P_FPROC ";"
#define P_CUR     "kyotocabinet/Cursor"
#define L_CUR     "L" P_CUR ";"
#define P_DB      "kyotocabinet/DB"
#define L_DB      "L" P_DB ";"
#define P_MR      "kyotocabinet/MapReduce"
#define L_MR      "L" P_MR ";"
#define P_VITER   "kyotocabinet/ValueIterator"
#define L_VITER   "L" P_VITER ";"

namespace kc = kyotocabinet;


/* precedent type declaration */
class SoftString;
class SoftArray;
class CursorBurrow;
class SoftCursor;
class SoftVisitor;
class SoftFileProcessor;
class SoftMapReduce;
typedef std::map<std::string, std::string> StringMap;
typedef std::vector<std::string> StringVector;


/* function prototypes */
static void throwruntime(JNIEnv* env, const char* message);
static void throwoutmem(JNIEnv* env);
static void throwillarg(JNIEnv* env);
static jstring newstring(JNIEnv* env, const char* str);
static jbyteArray newarray(JNIEnv* env, const char* buf, size_t size);
static jobject maptojhash(JNIEnv* env, const StringMap* map);
static jobject vectortojlist(JNIEnv* env, const StringVector* vec);
static kc::PolyDB* getdbcore(JNIEnv* env, jobject jdb);
static void throwdberror(JNIEnv* env, jobject jdb);
static SoftCursor* getcurcore(JNIEnv* env, jobject jcur);
static jobject getcurdb(JNIEnv* env, jobject jcur);


/* global variables */
const char* const p_err_children[] = {
  P_ERR "$XSUCCESS",
  P_ERR "$XNOIMPL",
  P_ERR "$XINVALID",
  P_ERR "$XNOREPOS",
  P_ERR "$XNOPERM",
  P_ERR "$XBROKEN",
  P_ERR "$XDUPREC",
  P_ERR "$XNOREC",
  P_ERR "$XLOGIC",
  P_ERR "$XSYSTEM",
  P_ERR "$XMISC",
};
jbyteArray obj_vis_nop;
jbyteArray obj_vis_remove;


/**
 * Generic options.
 */
enum GenericOption {
  GEXCEPTIONAL = 1 << 0,
  GCONCURRENT = 1 << 1
};


/**
 * Wrapper to treat a Java string as a C++ string.
 */
class SoftString {
 public:
  explicit SoftString(JNIEnv* env, jstring jstr) :
      env_(env), jstr_(jstr), str_(NULL), copied_(false) {
    if (jstr) {
      str_ = env_->GetStringUTFChars(jstr_, &copied_);
      if (!str_) {
        throwoutmem(env);
        throw std::bad_alloc();
      }
    } else {
      str_ = NULL;
    }
  }
  ~SoftString() {
    if (copied_) env_->ReleaseStringUTFChars(jstr_, str_);
  }
  const char* str() {
    return str_;
  }
 private:
  JNIEnv* env_;
  jstring jstr_;
  const char* str_;
  jboolean copied_;
};


/**
 * Wrapper to treat a Java byte array as a C++ byte array.
 */
class SoftArray {
 public:
  explicit SoftArray(JNIEnv* env, jbyteArray jary) :
      env_(env), jary_(jary), ary_(NULL), size_(0), copied_(false) {
    if (jary) {
      ary_ = env_->GetByteArrayElements(jary, &copied_);
      if (!ary_) {
        throwoutmem(env);
        throw std::bad_alloc();
      }
      size_ = env_->GetArrayLength(jary);
    } else {
      ary_ = NULL;
    }
  }
  ~SoftArray() {
    if (copied_) env_->ReleaseByteArrayElements(jary_, ary_, JNI_ABORT);
  }
  const char* ptr() {
    return (const char*)ary_;
  }
  size_t size() {
    return size_;
  }
 private:
  JNIEnv* env_;
  jbyteArray jary_;
  jbyte* ary_;
  size_t size_;
  jboolean copied_;
};


/**
 * Burrow of cursors no longer in use.
 */
class CursorBurrow {
 private:
  typedef std::vector<kc::PolyDB::Cursor*> CursorList;
 public:
  explicit CursorBurrow() : mlock_(), dcurs_() {}
  ~CursorBurrow() {
    sweap();
  }
  void sweap() {
    kc::ScopedSpinLock lock(&mlock_);
    if (dcurs_.size() > 0) {
      CursorList::iterator dit = dcurs_.begin();
      CursorList::iterator ditend = dcurs_.end();
      while (dit != ditend) {
        kc::PolyDB::Cursor* cur = *dit;
        delete cur;
        dit++;
      }
      dcurs_.clear();
    }
  }
  void deposit(kc::PolyDB::Cursor* cur) {
    kc::ScopedSpinLock lock(&mlock_);
    dcurs_.push_back(cur);
  }
 private:
  kc::SpinLock mlock_;
  CursorList dcurs_;
} g_curbur;


/**
 * Wrapper of a cursor.
 */
class SoftCursor {
 public:
  explicit SoftCursor(kc::PolyDB* db) : cur_(NULL) {
    cur_ = db->cursor();
  }
  ~SoftCursor() {
    if (cur_) g_curbur.deposit(cur_);
  }
  kc::PolyDB::Cursor* cur() {
    return cur_;
  }
  void disable() {
    delete cur_;
    cur_ = NULL;
  }
 private:
  kc::PolyDB::Cursor* cur_;
};


/**
 * Wrapper of a visitor.
 */
class SoftVisitor : public kc::PolyDB::Visitor {
 public:
  explicit SoftVisitor(JNIEnv* env, jobject jvisitor, bool writable) :
      env_(env), jvisitor_(jvisitor), writable_(writable),
      cls_vis_(0), id_vis_visit_full_(0), id_vis_visit_empty_(0),
      jrv_(NULL), rv_(NULL), jex_(NULL) {
    cls_vis_ = env->GetObjectClass(jvisitor_);
    id_vis_visit_full_ = env->GetMethodID(cls_vis_, "visit_full", "([B[B)[B");
    id_vis_visit_empty_ = env->GetMethodID(cls_vis_, "visit_empty", "([B)[B");
  }
  ~SoftVisitor() {
    cleanup();
  }
  jthrowable exception() {
    return jex_;
  }
 private:
  const char* visit_full(const char* kbuf, size_t ksiz,
                         const char* vbuf, size_t vsiz, size_t* sp) {
    cleanup();
    jbyteArray jkey = newarray(env_, kbuf, ksiz);
    jbyteArray jvalue = newarray(env_, vbuf, vsiz);
    jbyteArray jrv = (jbyteArray)env_->CallObjectMethod(jvisitor_, id_vis_visit_full_,
                                                        jkey, jvalue);
    jthrowable jex = env_->ExceptionOccurred();
    if (jex) {
      if (jex_) env_->DeleteLocalRef(jex_);
      jex_ = jex;
      env_->ExceptionClear();
    }
    env_->DeleteLocalRef(jkey);
    env_->DeleteLocalRef(jvalue);
    if (!jrv) return NOP;
    if (env_->IsSameObject(jrv, obj_vis_nop)) {
      env_->DeleteLocalRef(jrv);
      return NOP;
    }
    if (!writable_) {
      env_->DeleteLocalRef(jrv);
      throwruntime(env_, "confliction with the read-only parameter");
      jex = env_->ExceptionOccurred();
      if (jex) {
        if (jex_) env_->DeleteLocalRef(jex_);
        jex_ = jex;
        env_->ExceptionClear();
      }
      return NOP;
    }
    if (env_->IsSameObject(jrv, obj_vis_remove)) {
      env_->DeleteLocalRef(jrv);
      return REMOVE;
    }
    jrv_ = jrv;
    rv_ = new SoftArray(env_, jrv);
    *sp = rv_->size();
    return rv_->ptr();
  }
  const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp) {
    cleanup();
    jbyteArray jkey = newarray(env_, kbuf, ksiz);
    jbyteArray jrv = (jbyteArray)env_->CallObjectMethod(jvisitor_, id_vis_visit_empty_, jkey);
    jthrowable jex = env_->ExceptionOccurred();
    if (jex) {
      if (jex_) env_->DeleteLocalRef(jex_);
      jex_ = jex;
      env_->ExceptionClear();
    }
    env_->DeleteLocalRef(jkey);
    if (!jrv) return NOP;
    if (env_->IsSameObject(jrv, obj_vis_nop)) {
      env_->DeleteLocalRef(jrv);
      return NOP;
    }
    if (!writable_) {
      env_->DeleteLocalRef(jrv);
      throwruntime(env_, "confliction with the read-only parameter");
      jex = env_->ExceptionOccurred();
      if (jex) {
        if (jex_) env_->DeleteLocalRef(jex_);
        jex_ = jex;
        env_->ExceptionClear();
      }
      return NOP;
    }
    if (env_->IsSameObject(jrv, obj_vis_remove)) {
      env_->DeleteLocalRef(jrv);
      return REMOVE;
    }
    jrv_ = jrv;
    rv_ = new SoftArray(env_, jrv);
    *sp = rv_->size();
    return rv_->ptr();
  }
  void cleanup() {
    delete rv_;
    rv_ = NULL;
    if (jrv_) {
      env_->DeleteLocalRef(jrv_);
      jrv_ = NULL;
    }
  }
  JNIEnv* env_;
  jobject jvisitor_;
  bool writable_;
  jclass cls_vis_;
  jmethodID id_vis_visit_full_;
  jmethodID id_vis_visit_empty_;
  jbyteArray jrv_;
  SoftArray* rv_;
  jthrowable jex_;
};


/**
 * Wrapper of a file processor.
 */
class SoftFileProcessor : public kc::PolyDB::FileProcessor {
 public:
  explicit SoftFileProcessor(JNIEnv* env, jobject jproc) :
      env_(env), jproc_(jproc), cls_fproc_(0), id_fproc_process_(0), jex_(NULL) {
    cls_fproc_ = env->GetObjectClass(jproc_);
    id_fproc_process_ = env->GetMethodID(cls_fproc_, "process", "(" L_STR "JJ)Z");
  }
  jthrowable exception() {
    return jex_;
  }
 private:
  bool process(const std::string& path, int64_t count, int64_t size) {
    jstring jpath = newstring(env_, path.c_str());
    bool rv = env_->CallBooleanMethod(jproc_, id_fproc_process_, jpath, count, size);
    jthrowable jex = env_->ExceptionOccurred();
    if (jex) {
      jex_ = jex;
      env_->ExceptionClear();
    }
    env_->DeleteLocalRef(jpath);
    return rv;
  }
  JNIEnv* env_;
  jobject jproc_;
  jclass cls_fproc_;
  jmethodID id_fproc_process_;
  jthrowable jex_;
};


/**
 * Wrapper of a MapReduce framework.
 */
class SoftMapReduce : public kc::MapReduce {
 public:
  explicit SoftMapReduce(JNIEnv* env, jobject jmr) :
      env_(env), jmr_(jmr), cls_mr_(0), id_mr_map_(0), id_mr_reduce_(0),
      id_mr_preproc_(0), id_mr_midproc_(0), id_mr_postproc_(0),
      cls_viter_(0), id_viter_init_(0), jex_(NULL) {
    cls_mr_ = env->GetObjectClass(jmr_);
    id_mr_map_ = env->GetMethodID(cls_mr_, "map", "([B[B)Z");
    id_mr_reduce_ = env->GetMethodID(cls_mr_, "reduce", "([B" L_VITER ")Z");
    id_mr_preproc_ = env->GetMethodID(cls_mr_, "preprocess", "()Z");
    id_mr_midproc_ = env->GetMethodID(cls_mr_, "midprocess", "()Z");
    id_mr_postproc_ = env->GetMethodID(cls_mr_, "postprocess", "()Z");
    cls_viter_ = env->FindClass(P_VITER);
    id_viter_init_ = env->GetMethodID(cls_viter_, "<init>", "()V");
    jfieldID id_mr_ptr = env->GetFieldID(cls_mr_, "ptr_", "J");
    env->SetLongField(jmr_, id_mr_ptr, (intptr_t)this);
  }
  jthrowable exception() {
    return jex_;
  }
  bool emit_public(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
    return emit(kbuf, ksiz, vbuf, vsiz);
  }
 private:
  bool map(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
    jbyteArray jkey = newarray(env_, kbuf, ksiz);
    jbyteArray jvalue = newarray(env_, vbuf, vsiz);
    bool rv = env_->CallBooleanMethod(jmr_, id_mr_map_, jkey, jvalue);
    env_->DeleteLocalRef(jkey);
    env_->DeleteLocalRef(jvalue);
    jthrowable jex = env_->ExceptionOccurred();
    if (jex) {
      if (jex_) env_->DeleteLocalRef(jex_);
      jex_ = jex;
      env_->ExceptionClear();
    }
    return rv;
  }
  bool reduce(const char* kbuf, size_t ksiz, ValueIterator* iter) {
    jbyteArray jkey = newarray(env_, kbuf, ksiz);
    jobject jviter = env_->NewObject(cls_viter_, id_viter_init_);
    jfieldID id_viter_ptr = env_->GetFieldID(cls_viter_, "ptr_", "J");
    env_->SetLongField(jviter, id_viter_ptr, (intptr_t)iter);
    bool rv = env_->CallBooleanMethod(jmr_, id_mr_reduce_, jkey, jviter);
    env_->DeleteLocalRef(jviter);
    env_->DeleteLocalRef(jkey);
    jthrowable jex = env_->ExceptionOccurred();
    if (jex) {
      if (jex_) env_->DeleteLocalRef(jex_);
      jex_ = jex;
      env_->ExceptionClear();
    }
    return rv;
  }
  bool preprocess() {
    bool rv = env_->CallBooleanMethod(jmr_, id_mr_preproc_);
    jthrowable jex = env_->ExceptionOccurred();
    if (jex) {
      if (jex_) env_->DeleteLocalRef(jex_);
      jex_ = jex;
      env_->ExceptionClear();
    }
    return rv;
  }
  bool midprocess() {
    bool rv = env_->CallBooleanMethod(jmr_, id_mr_midproc_);
    jthrowable jex = env_->ExceptionOccurred();
    if (jex) {
      if (jex_) env_->DeleteLocalRef(jex_);
      jex_ = jex;
      env_->ExceptionClear();
    }
    return rv;
  }
  bool postprocess() {
    bool rv = env_->CallBooleanMethod(jmr_, id_mr_postproc_);
    jthrowable jex = env_->ExceptionOccurred();
    if (jex) {
      if (jex_) env_->DeleteLocalRef(jex_);
      jex_ = jex;
      env_->ExceptionClear();
    }
    return rv;
  }
  bool log(const char* name, const char* message) {
    return true;
  }
 private:
  JNIEnv* env_;
  jobject jmr_;
  jclass cls_mr_;
  jmethodID id_mr_map_;
  jmethodID id_mr_reduce_;
  jmethodID id_mr_preproc_;
  jmethodID id_mr_midproc_;
  jmethodID id_mr_postproc_;
  jclass cls_viter_;
  jmethodID id_viter_init_;
  jthrowable jex_;
};


/**
 * Throw a runtime error.
 */
static void throwruntime(JNIEnv* env, const char* message) {
  jclass cls = env->FindClass("java/lang/RuntimeException");
  env->ThrowNew(cls, message);
}


/**
 * Throw the out-of-memory error.
 */
static void throwoutmem(JNIEnv* env) {
  jclass cls = env->FindClass("java/lang/OutOfMemoryError");
  env->ThrowNew(cls, "out of memory");
}


/**
 * Throw the illegal argument exception.
 */
static void throwillarg(JNIEnv* env) {
  jclass cls = env->FindClass("java/lang/IllegalArgumentException");
  env->ThrowNew(cls, "illegal argument");
}


/**
 * Create a new string.
 */
static jstring newstring(JNIEnv* env, const char* str) {
  jstring jstr = env->NewStringUTF(str);
  if (!jstr) {
    throwoutmem(env);
    throw std::bad_alloc();
  }
  return jstr;
}


/**
 * Create a new byte array.
 */
static jbyteArray newarray(JNIEnv* env, const char* buf, size_t size) {
  jbyteArray jbuf = env->NewByteArray(size);
  if (!jbuf) {
    throwoutmem(env);
    throw std::bad_alloc();
  }
  env->SetByteArrayRegion(jbuf, 0, size, (jbyte *)buf);
  return jbuf;
}


/**
 * Convert an internal map to a Java map.
 */
static jobject maptojhash(JNIEnv* env, const StringMap* map) {
  jclass cls_hm = env->FindClass("java/util/HashMap");
  jmethodID id_hm_init = env->GetMethodID(cls_hm, "<init>", "(I)V");
  jmethodID id_hm_put = env->GetMethodID(cls_hm, "put", "(" L_OBJ L_OBJ ")" L_OBJ);
  jobject jhash = env->NewObject(cls_hm, id_hm_init, map->size() * 2 + 1);
  StringMap::const_iterator it = map->begin();
  StringMap::const_iterator itend = map->end();
  while (it != itend) {
    jstring jkey = newstring(env, it->first.c_str());
    jstring jvalue = newstring(env, it->second.c_str());
    env->CallObjectMethod(jhash, id_hm_put, jkey, jvalue);
    env->DeleteLocalRef(jkey);
    env->DeleteLocalRef(jvalue);
    it++;
  }
  return jhash;
}


/**
 * Convert an internal vector to a Java list.
 */
static jobject vectortojlist(JNIEnv* env, const StringVector* vec) {
  jclass cls_al = env->FindClass("java/util/ArrayList");
  jmethodID id_al_init = env->GetMethodID(cls_al, "<init>", "(I)V");
  jmethodID id_al_add = env->GetMethodID(cls_al, "add", "(" L_OBJ ")Z");
  jobject jlist = env->NewObject(cls_al, id_al_init, vec->size());
  StringVector::const_iterator it = vec->begin();
  StringVector::const_iterator itend = vec->end();
  while (it != itend) {
    jstring jstr = newstring(env, it->c_str());
    env->CallObjectMethod(jlist, id_al_add, jstr);
    env->DeleteLocalRef(jstr);
    it++;
  }
  return jlist;
}


/**
 * Convert the pointer to the internal data of a database object.
 */
static kc::PolyDB* getdbcore(JNIEnv* env, jobject jdb) {
  jclass cls_db = env->GetObjectClass(jdb);
  jfieldID id_db_ptr = env->GetFieldID(cls_db, "ptr_", "J");
  return (kc::PolyDB*)(intptr_t)env->GetLongField(jdb, id_db_ptr);
}


/**
 * Throw the exception of an error code.
 */
static void throwdberror(JNIEnv* env, jobject jdb) {
  jclass cls_db = env->GetObjectClass(jdb);
  jfieldID id_db_exbits = env->GetFieldID(cls_db, "exbits_", "I");
  uint32_t exbits = env->GetLongField(jdb, id_db_exbits);
  if (exbits == 0) return;
  kc::PolyDB* db = getdbcore(env, jdb);
  kc::PolyDB::Error err = db->error();
  uint32_t code = err.code();
  if (exbits & (1 << code)) {
    jclass cls = env->FindClass(p_err_children[code]);
    std::string expr = kc::strprintf("%u: %s", code, err.message());
    env->ThrowNew(cls, expr.c_str());
  }
}


/**
 * Convert the pointer to the internal data of a cursor object.
 */
static SoftCursor* getcurcore(JNIEnv* env, jobject jcur) {
  jclass cls_cur = env->GetObjectClass(jcur);
  jfieldID id_cur_ptr = env->GetFieldID(cls_cur, "ptr_", "J");
  return (SoftCursor*)(intptr_t)env->GetLongField(jcur, id_cur_ptr);
}


/**
 * Get the inner database object of a cursor object.
 */
static jobject getcurdb(JNIEnv* env, jobject jcur) {
  jclass cls_cur = env->GetObjectClass(jcur);
  jfieldID id_cur_db = env->GetFieldID(cls_cur, "db_", L_DB);
  return env->GetObjectField(jcur, id_cur_db);
}


/**
 * Implementation of init_visitor_NOP.
 */
JNIEXPORT jbyteArray JNICALL Java_kyotocabinet_Utility_init_1visitor_1NOP
(JNIEnv* env, jclass cls) {
  try {
    char buf[kc::NUMBUFSIZ];
    size_t size = std::sprintf(buf, "[NOP]");
    obj_vis_nop = (jbyteArray)env->NewGlobalRef((jobject)newarray(env, buf, size));
    return obj_vis_nop;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of init_visitor_REMOVE.
 */
JNIEXPORT jbyteArray JNICALL Java_kyotocabinet_Utility_init_1visitor_1REMOVE
(JNIEnv* env, jclass cls) {
  try {
    char buf[kc::NUMBUFSIZ];
    size_t size = std::sprintf(buf, "[REMOVE]");
    obj_vis_remove = (jbyteArray)env->NewGlobalRef((jobject)newarray(env, buf, size));
    return obj_vis_remove;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of atoi.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_Utility_atoi
(JNIEnv* env, jclass cls, jstring jstr) {
  try {
    if (!jstr) {
      throwillarg(env);
      return 0;
    }
    SoftString str(env, jstr);
    return kc::atoi(str.str());
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of atoix.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_Utility_atoix
(JNIEnv* env, jclass cls, jstring jstr) {
  try {
    if (!jstr) {
      throwillarg(env);
      return 0;
    }
    SoftString str(env, jstr);
    return kc::atoix(str.str());
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of atof.
 */
JNIEXPORT jdouble JNICALL Java_kyotocabinet_Utility_atof
(JNIEnv* env, jclass cls, jstring jstr) {
  try {
    if (!jstr) {
      throwillarg(env);
      return 0;
    }
    SoftString str(env, jstr);
    return kc::atof(str.str());
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of hash_murmur.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_Utility_hash_1murmur
(JNIEnv* env, jclass cls, jbyteArray jary) {
  try {
    if (!jary) {
      throwillarg(env);
      return 0;
    }
    SoftArray ary(env, jary);
    return kc::hashmurmur(ary.ptr(), ary.size());
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of hash_fnv.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_Utility_hash_1fnv
(JNIEnv* env, jclass cls, jbyteArray jary) {
  try {
    if (!jary) {
      throwillarg(env);
      return 0;
    }
    SoftArray ary(env, jary);
    return kc::hashfnv(ary.ptr(), ary.size());
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of levdist.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_Utility_levdist___3B_3B
(JNIEnv* env, jclass cls, jbyteArray ja, jbyteArray jb) {
  try {
    if (!ja || !jb) {
      throwillarg(env);
      return 0;
    }
    SoftArray aary(env, ja);
    SoftArray bary(env, jb);
    return kc::memdist(aary.ptr(), aary.size(), bary.ptr(), bary.size());
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of levdist.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_Utility_levdist__Ljava_lang_String_2Ljava_lang_String_2
(JNIEnv* env, jclass cls, jstring ja, jstring jb) {
  try {
    if (!ja || !jb) {
      throwillarg(env);
      return 0;
    }
    SoftString astr(env, ja);
    SoftString bstr(env, jb);
    return kc::strutfdist(astr.str(), bstr.str());
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of remove_recursively.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Utility_remove_1files_1recursively
(JNIEnv* env, jclass cls, jstring jpath) {
  if (!jpath) {
    throwillarg(env);
    return false;
  }
  SoftString path(env, jpath);
  return kc::File::remove_recursively(path.str());
}


/**
 * Implementation of version.
 */
JNIEXPORT jstring JNICALL Java_kyotocabinet_Utility_version
(JNIEnv* env, jclass cls) {
  try {
    return newstring(env, kc::VERSION);
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of codename.
 */
JNIEXPORT jstring JNICALL Java_kyotocabinet_Error_codename
(JNIEnv* env, jclass cls, jint code) {
  try {
    return newstring(env, kc::PolyDB::Error::codename((kyotocabinet::PolyDB::Error::Code)code));
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of disable.
 */
JNIEXPORT void JNICALL Java_kyotocabinet_Cursor_disable
(JNIEnv* env, jobject jself) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return;
    cur->disable();
  } catch (std::exception& e) {}
}


/**
 * Implementation of accept.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Cursor_accept
(JNIEnv* env, jobject jself, jobject jvisitor, jboolean writable, jboolean step) {
  try {
    if (!jvisitor) {
      throwillarg(env);
      return false;
    }
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return false;
    SoftVisitor visitor(env, jvisitor, writable);
    bool rv = icur->accept(&visitor, writable, step);
    jthrowable jex = visitor.exception();
    if (jex) {
      env->Throw(jex);
      return false;
    }
    if (rv) return true;
    throwdberror(env, getcurdb(env, jself));
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of set_value.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Cursor_set_1value
(JNIEnv* env, jobject jself, jbyteArray jvalue, jboolean step) {
  try {
    if (!jvalue) {
      throwillarg(env);
      return false;
    }
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return false;
    SoftArray value(env, jvalue);
    bool rv = icur->set_value(value.ptr(), value.size(), step);
    if (rv) return true;
    throwdberror(env, getcurdb(env, jself));
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of remove.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Cursor_remove
(JNIEnv* env, jobject jself) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return false;
    bool rv = icur->remove();
    if (rv) return true;
    throwdberror(env, getcurdb(env, jself));
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of get_key.
 */
JNIEXPORT jbyteArray JNICALL Java_kyotocabinet_Cursor_get_1key
(JNIEnv* env, jobject jself, jboolean step) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return NULL;
    size_t ksiz;
    char* kbuf = icur->get_key(&ksiz, step);
    if (!kbuf) {
      throwdberror(env, getcurdb(env, jself));
      return NULL;
    }
    jbyteArray jkey = newarray(env, kbuf, ksiz);
    delete[] kbuf;
    return jkey;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of get_value.
 */
JNIEXPORT jbyteArray JNICALL Java_kyotocabinet_Cursor_get_1value
(JNIEnv* env, jobject jself, jboolean step) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return NULL;
    size_t vsiz;
    char* vbuf = icur->get_value(&vsiz, step);
    if (!vbuf) {
      throwdberror(env, getcurdb(env, jself));
      return NULL;
    }
    jbyteArray jvalue = newarray(env, vbuf, vsiz);
    delete[] vbuf;
    return jvalue;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of get.
 */
JNIEXPORT jobjectArray JNICALL Java_kyotocabinet_Cursor_get
(JNIEnv* env, jobject jself, jboolean step) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return NULL;
    const char* vbuf;
    size_t ksiz, vsiz;
    char* kbuf = icur->get(&ksiz, &vbuf, &vsiz, step);
    if (!kbuf) {
      throwdberror(env, getcurdb(env, jself));
      return NULL;
    }
    jbyteArray jkey = newarray(env, kbuf, ksiz);
    jbyteArray jvalue = newarray(env, vbuf, vsiz);
    delete[] kbuf;
    jclass cls_byteary = env->FindClass("[B");
    jobjectArray jrec = env->NewObjectArray(2, cls_byteary, NULL);
    env->SetObjectArrayElement(jrec, 0, jkey);
    env->SetObjectArrayElement(jrec, 1, jvalue);
    env->DeleteLocalRef(jkey);
    env->DeleteLocalRef(jvalue);
    return jrec;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of seize.
 */
JNIEXPORT jobjectArray JNICALL Java_kyotocabinet_Cursor_seize
(JNIEnv* env, jobject jself) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return NULL;
    const char* vbuf;
    size_t ksiz, vsiz;
    char* kbuf = icur->seize(&ksiz, &vbuf, &vsiz);
    if (!kbuf) {
      throwdberror(env, getcurdb(env, jself));
      return NULL;
    }
    jbyteArray jkey = newarray(env, kbuf, ksiz);
    jbyteArray jvalue = newarray(env, vbuf, vsiz);
    delete[] kbuf;
    jclass cls_byteary = env->FindClass("[B");
    jobjectArray jrec = env->NewObjectArray(2, cls_byteary, NULL);
    env->SetObjectArrayElement(jrec, 0, jkey);
    env->SetObjectArrayElement(jrec, 1, jvalue);
    env->DeleteLocalRef(jkey);
    env->DeleteLocalRef(jvalue);
    return jrec;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of jump.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Cursor_jump__
(JNIEnv* env, jobject jself) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return false;
    bool rv = icur->jump();
    if (rv) return true;
    throwdberror(env, getcurdb(env, jself));
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of jump.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Cursor_jump___3B
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  try {
    if (!jkey) {
      throwillarg(env);
      return false;
    }
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return false;
    SoftArray key(env, jkey);
    bool rv = icur->jump(key.ptr(), key.size());
    if (rv) return true;
    throwdberror(env, getcurdb(env, jself));
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of jump_back.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Cursor_jump_1back__
(JNIEnv* env, jobject jself) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return false;
    bool rv = icur->jump_back();
    if (rv) return true;
    throwdberror(env, getcurdb(env, jself));
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of jump_back.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Cursor_jump_1back___3B
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  try {
    if (!jkey) {
      throwillarg(env);
      return false;
    }
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return false;
    SoftArray key(env, jkey);
    bool rv = icur->jump_back(key.ptr(), key.size());
    if (rv) return true;
    throwdberror(env, getcurdb(env, jself));
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of step.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Cursor_step
(JNIEnv* env, jobject jself) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return false;
    bool rv = icur->step();
    if (rv) return true;
    throwdberror(env, getcurdb(env, jself));
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of step_back.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_Cursor_step_1back
(JNIEnv* env, jobject jself) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return false;
    bool rv = icur->step_back();
    if (rv) return true;
    throwdberror(env, getcurdb(env, jself));
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of error.
 */
JNIEXPORT jthrowable JNICALL Java_kyotocabinet_Cursor_error
(JNIEnv* env, jobject jself) {
  try {
    jclass cls_err = env->FindClass(P_ERR);
    jmethodID id_err_init = env->GetMethodID(cls_err, "<init>", "(I" L_STR ")V");
    SoftCursor* cur = getcurcore(env, jself);
    kc::PolyDB::Cursor* icur = cur->cur();
    if (!icur) return NULL;
    kc::PolyDB::Error err = cur->cur()->error();
    jstring jmessage = newstring(env, err.message());
    jobject jerr = env->NewObject(cls_err, id_err_init, err.code(), jmessage);
    env->DeleteLocalRef(jmessage);
    return (jthrowable)jerr;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of db.
 */
JNIEXPORT jobject JNICALL Java_kyotocabinet_Cursor_db
(JNIEnv* env, jobject jself) {
  try {
    return getcurdb(env, jself);
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of initialize.
 */
JNIEXPORT void JNICALL Java_kyotocabinet_Cursor_initialize
(JNIEnv* env, jobject jself, jobject jdb) {
  try {
    if (!jdb) {
      throwillarg(env);
      return;
    }
    jclass cls_cur = env->GetObjectClass(jself);
    jfieldID id_cur_ptr = env->GetFieldID(cls_cur, "ptr_", "J");
    jfieldID id_cur_db = env->GetFieldID(cls_cur, "db_", L_DB);
    kc::PolyDB* db = getdbcore(env, jdb);
    g_curbur.sweap();
    SoftCursor* cur = new SoftCursor(db);
    env->SetLongField(jself, id_cur_ptr, (intptr_t)cur);
    env->SetObjectField(jself, id_cur_db, jdb);
  } catch (std::exception& e) {}
}


/**
 * Implementation of destruct.
 */
JNIEXPORT void JNICALL Java_kyotocabinet_Cursor_destruct
(JNIEnv* env, jobject jself) {
  try {
    SoftCursor* cur = getcurcore(env, jself);
    delete cur;
  } catch (std::exception& e) {}
}


/**
 * Implementation of error.
 */
JNIEXPORT jthrowable JNICALL Java_kyotocabinet_DB_error
(JNIEnv* env, jobject jself) {
  try {
    jclass cls_err = env->FindClass(P_ERR);
    jmethodID id_err_init = env->GetMethodID(cls_err, "<init>", "(I" L_STR ")V");
    kc::PolyDB* db = getdbcore(env, jself);
    kc::PolyDB::Error err = db->error();
    jstring jmessage = newstring(env, err.message());
    jobject jerr = env->NewObject(cls_err, id_err_init, err.code(), jmessage);
    env->DeleteLocalRef(jmessage);
    return (jthrowable)jerr;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of open.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_open
(JNIEnv* env, jobject jself, jstring jpath, jint mode) {
  try {
    if (!jpath) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftString path(env, jpath);
    bool rv = db->open(path.str(), mode);
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of close.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_close
(JNIEnv* env, jobject jself) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    g_curbur.sweap();
    bool rv = db->close();
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of accept.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_accept
(JNIEnv* env, jobject jself, jbyteArray jkey, jobject jvisitor, jboolean writable) {
  try {
    if (!jkey || !jvisitor) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    SoftVisitor visitor(env, jvisitor, writable);
    bool rv = db->accept(key.ptr(), key.size(), &visitor, writable);
    jthrowable jex = visitor.exception();
    if (jex) {
      env->Throw(jex);
      return false;
    }
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of accept_bulk.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_accept_1bulk
(JNIEnv* env, jobject jself, jobjectArray jkeys, jobject jvisitor, jboolean writable) {
  try {
    if (!jkeys || !jvisitor) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    size_t knum = env->GetArrayLength(jkeys);
    StringVector keys;
    keys.reserve(knum);
    for (size_t i = 0; i < knum; i++) {
      jbyteArray jkey = (jbyteArray)env->GetObjectArrayElement(jkeys, i);
      if (jkey) {
        SoftArray key(env, jkey);
        keys.push_back(std::string(key.ptr(), key.size()));
      }
      env->DeleteLocalRef(jkey);
    }
    SoftVisitor visitor(env, jvisitor, writable);
    bool rv = db->accept_bulk(keys, &visitor, writable);
    jthrowable jex = visitor.exception();
    if (jex) {
      env->Throw(jex);
      return false;
    }
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of iterate.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_iterate
(JNIEnv* env, jobject jself, jobject jvisitor, jboolean writable) {
  try {
    if (!jvisitor) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftVisitor visitor(env, jvisitor, writable);
    bool rv = db->iterate(&visitor, writable);
    jthrowable jex = visitor.exception();
    if (jex) {
      env->Throw(jex);
      return false;
    }
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of set.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_set
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue) {
  try {
    if (!jkey || !jvalue) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    SoftArray value(env, jvalue);
    bool rv = db->set(key.ptr(), key.size(), value.ptr(), value.size());
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of add.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_add
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue) {
  try {
    if (!jkey || !jvalue) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    SoftArray value(env, jvalue);
    bool rv = db->add(key.ptr(), key.size(), value.ptr(), value.size());
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of replace.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_replace
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue) {
  try {
    if (!jkey || !jvalue) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    SoftArray value(env, jvalue);
    bool rv = db->replace(key.ptr(), key.size(), value.ptr(), value.size());
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of append.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_append
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue) {
  try {
    if (!jkey || !jvalue) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    SoftArray value(env, jvalue);
    bool rv = db->append(key.ptr(), key.size(), value.ptr(), value.size());
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of increment.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_DB_increment
(JNIEnv* env, jobject jself, jbyteArray jkey, jlong num, jlong orig) {
  try {
    if (!jkey) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    num = db->increment(key.ptr(), key.size(), num, orig);
    if (num == kc::INT64MIN) throwdberror(env, jself);
    return num;
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of increment_double.
 */
JNIEXPORT jdouble JNICALL Java_kyotocabinet_DB_increment_1double
(JNIEnv* env, jobject jself, jbyteArray jkey, jdouble num, jdouble orig) {
  try {
    if (!jkey) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    num = db->increment_double(key.ptr(), key.size(), num, orig);
    if (kc::chknan(num)) throwdberror(env, jself);
    return num;
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of cas.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_cas
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray joval, jbyteArray jnval) {
  try {
    if (!jkey) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    SoftArray oval(env, joval);
    SoftArray nval(env, jnval);
    bool rv = db->cas(key.ptr(), key.size(), oval.ptr(), oval.size(), nval.ptr(), nval.size());
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of remove.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_remove
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  try {
    if (!jkey) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    bool rv = db->remove(key.ptr(), key.size());
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of get.
 */
JNIEXPORT jbyteArray JNICALL Java_kyotocabinet_DB_get
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  try {
    if (!jkey) {
      throwillarg(env);
      return NULL;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    size_t vsiz;
    char* vbuf = db->get(key.ptr(), key.size(), &vsiz);
    if (!vbuf) {
      throwdberror(env, jself);
      return NULL;
    }
    jbyteArray jvalue = newarray(env, vbuf, vsiz);
    delete[] vbuf;
    return jvalue;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of check.
 */
JNIEXPORT jint JNICALL Java_kyotocabinet_DB_check
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  try {
    if (!jkey) {
      throwillarg(env);
      return -1;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    int32_t vsiz = db->check(key.ptr(), key.size());
    if (vsiz < 0) {
      throwdberror(env, jself);
      return -1;
    }
    return vsiz;
  } catch (std::exception& e) {
    return -1;
  }
}


/**
 * Implementation of seize.
 */
JNIEXPORT jbyteArray JNICALL Java_kyotocabinet_DB_seize
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  try {
    if (!jkey) {
      throwillarg(env);
      return NULL;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftArray key(env, jkey);
    size_t vsiz;
    char* vbuf = db->seize(key.ptr(), key.size(), &vsiz);
    if (!vbuf) {
      throwdberror(env, jself);
      return NULL;
    }
    jbyteArray jvalue = newarray(env, vbuf, vsiz);
    delete[] vbuf;
    return jvalue;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of set_bulk.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_DB_set_1bulk
(JNIEnv* env, jobject jself, jobjectArray jrecs, jboolean atomic) {
  try {
    if (!jrecs) {
      throwillarg(env);
      return -1;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    size_t rnum = env->GetArrayLength(jrecs);
    StringMap recs;
    for (size_t i = 0; i + 1 < rnum; i += 2) {
      jbyteArray jkey = (jbyteArray)env->GetObjectArrayElement(jrecs, i);
      jbyteArray jvalue = (jbyteArray)env->GetObjectArrayElement(jrecs, i + 1);
      if (jkey && jvalue) {
        SoftArray key(env, jkey);
        SoftArray value(env, jvalue);
        recs[std::string(key.ptr(), key.size())] = std::string(value.ptr(), value.size());
      }
      env->DeleteLocalRef(jkey);
      env->DeleteLocalRef(jvalue);
    }
    int64_t rv = db->set_bulk(recs, atomic);
    if (rv < 0) {
      throwdberror(env, jself);
      return -1;
    }
    return rv;
  } catch (std::exception& e) {
    return -1;
  }
}


/**
 * Implementation of remove_bulk.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_DB_remove_1bulk
(JNIEnv* env, jobject jself, jobjectArray jkeys, jboolean atomic) {
  try {
    if (!jkeys) {
      throwillarg(env);
      return -1;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    size_t knum = env->GetArrayLength(jkeys);
    StringVector keys;
    keys.reserve(knum);
    for (size_t i = 0; i < knum; i++) {
      jbyteArray jkey = (jbyteArray)env->GetObjectArrayElement(jkeys, i);
      if (jkey) {
        SoftArray key(env, jkey);
        keys.push_back(std::string(key.ptr(), key.size()));
      }
      env->DeleteLocalRef(jkey);
    }
    int64_t rv = db->remove_bulk(keys, atomic);
    if (rv < 0) {
      throwdberror(env, jself);
      return -1;
    }
    return rv;
  } catch (std::exception& e) {
    return (jlong)NULL;
  }
}


/**
 * Implementation of get_bulk.
 */
JNIEXPORT jobjectArray JNICALL Java_kyotocabinet_DB_get_1bulk
(JNIEnv* env, jobject jself, jobjectArray jkeys, jboolean atomic) {
  try {
    if (!jkeys) {
      throwillarg(env);
      return NULL;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    size_t knum = env->GetArrayLength(jkeys);
    StringVector keys;
    keys.reserve(knum);
    for (size_t i = 0; i < knum; i++) {
      jbyteArray jkey = (jbyteArray)env->GetObjectArrayElement(jkeys, i);
      if (jkey) {
        SoftArray key(env, jkey);
        keys.push_back(std::string(key.ptr(), key.size()));
      }
      env->DeleteLocalRef(jkey);
    }
    StringMap recs;
    if (db->get_bulk(keys, &recs, atomic) < 0) {
      throwdberror(env, jself);
      return NULL;
    }
    size_t rnum = recs.size();
    jclass cls_byteary = env->FindClass("[B");
    jobjectArray jrec = env->NewObjectArray(rnum * 2, cls_byteary, NULL);
    StringMap::iterator it = recs.begin();
    StringMap::iterator itend = recs.end();
    size_t ridx = 0;
    while (it != itend) {
      jbyteArray jkey = newarray(env, it->first.data(), it->first.size());
      jbyteArray jvalue = newarray(env, it->second.data(), it->second.size());
      env->SetObjectArrayElement(jrec, ridx++, jkey);
      env->SetObjectArrayElement(jrec, ridx++, jvalue);
      env->DeleteLocalRef(jkey);
      env->DeleteLocalRef(jvalue);
      it++;
    }
    return jrec;
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of clear.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_clear
(JNIEnv* env, jobject jself) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    bool rv = db->clear();
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of synchronize.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_synchronize
(JNIEnv* env, jobject jself, jboolean hard, jobject jproc) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    if (!jproc) return db->synchronize(hard);
    SoftFileProcessor proc(env, jproc);
    bool rv = db->synchronize(hard, &proc);
    jthrowable jex = proc.exception();
    if (jex) {
      env->Throw(jex);
      return false;
    }
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of occupy.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_occupy
(JNIEnv* env, jobject jself, jboolean writable, jobject jproc) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    if (!jproc) return db->occupy(writable);
    SoftFileProcessor proc(env, jproc);
    bool rv = db->occupy(writable, &proc);
    jthrowable jex = proc.exception();
    if (jex) {
      env->Throw(jex);
      return false;
    }
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of copy.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_copy
(JNIEnv* env, jobject jself, jstring jdest) {
  try {
    if (!jdest) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftString dest(env, jdest);
    bool rv = db->copy(dest.str());
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of begin_trnasaction.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_begin_1transaction
(JNIEnv* env, jobject jself, jboolean hard) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    bool rv = db->begin_transaction(hard);
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of end_transaction.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_end_1transaction
(JNIEnv* env, jobject jself, jboolean commit) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    bool rv = db->end_transaction(commit);
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of dump_snapshot.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_dump_1snapshot
(JNIEnv* env, jobject jself, jstring jdest) {
  try {
    if (!jdest) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftString dest(env, jdest);
    bool rv = db->dump_snapshot(dest.str());
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of load_snapshot.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_load_1snapshot
(JNIEnv* env, jobject jself, jstring jsrc) {
  try {
    if (!jsrc) {
      throwillarg(env);
      return false;
    }
    kc::PolyDB* db = getdbcore(env, jself);
    SoftString src(env, jsrc);
    bool rv = db->load_snapshot(src.str());
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of count.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_DB_count
(JNIEnv* env, jobject jself) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    int64_t count = db->count();
    if (count < 0) throwdberror(env, jself);
    return count;
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of size.
 */
JNIEXPORT jlong JNICALL Java_kyotocabinet_DB_size
(JNIEnv* env, jobject jself) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    int64_t size = db->size();
    if (size < 0) throwdberror(env, jself);
    return size;
  } catch (std::exception& e) {
    return 0;
  }
}


/**
 * Implementation of path.
 */
JNIEXPORT jstring JNICALL Java_kyotocabinet_DB_path
(JNIEnv* env, jobject jself) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    std::string path = db->path().c_str();
    if (path.empty()) {
      throwdberror(env, jself);
      return NULL;
    }
    return newstring(env, path.c_str());
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of status.
 */
JNIEXPORT jobject JNICALL Java_kyotocabinet_DB_status
(JNIEnv* env, jobject jself) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    StringMap status;
    if (!db->status(&status)) {
      throwdberror(env, jself);
      return NULL;
    }
    return maptojhash(env, &status);
  } catch (std::exception& e) {
    return NULL;
  }
}


/*
 * Implementation of match_prefix.
 */
JNIEXPORT jobject JNICALL Java_kyotocabinet_DB_match_1prefix
(JNIEnv* env, jobject jself, jstring jprefix, jlong max) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    SoftString prefix(env, jprefix);
    StringVector keys;
    if (db->match_prefix(prefix.str(), &keys, max) == -1) {
      throwdberror(env, jself);
      return NULL;
    }
    return vectortojlist(env, &keys);
  } catch (std::exception& e) {
    return NULL;
  }
}


/*
 * Implementation of match_regex.
 */
JNIEXPORT jobject JNICALL Java_kyotocabinet_DB_match_1regex
(JNIEnv* env, jobject jself, jstring jregex, jlong max) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    SoftString regex(env, jregex);
    StringVector keys;
    if (db->match_regex(regex.str(), &keys, max) == -1) {
      throwdberror(env, jself);
      return NULL;
    }
    return vectortojlist(env, &keys);
  } catch (std::exception& e) {
    return NULL;
  }
}


/*
 * Implementation of match_similar.
 */
JNIEXPORT jobject JNICALL Java_kyotocabinet_DB_match_1similar
(JNIEnv* env, jobject jself, jstring jorigin, jlong range, jboolean utf, jlong max) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    SoftString origin(env, jorigin);
    StringVector keys;
    if (db->match_similar(origin.str(), range, utf, &keys, max) == -1) {
      throwdberror(env, jself);
      return NULL;
    }
    return vectortojlist(env, &keys);
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of merge.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_DB_merge
(JNIEnv* env, jobject jself, jobjectArray jsrcary, jint mode) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    size_t srcnum = env->GetArrayLength(jsrcary);
    if (srcnum < 1) return true;
    kc::BasicDB** srcary = new kc::BasicDB*[srcnum];
    for (size_t i = 0; i < srcnum; i++) {
      jobject jsrcdb = env->GetObjectArrayElement(jsrcary, i);
      if (!jsrcdb) {
        delete[] srcary;
        return false;
      }
      srcary[i] = getdbcore(env, jsrcdb);
    }
    bool rv = db->merge(srcary, srcnum, (kc::PolyDB::MergeMode)mode);
    delete[] srcary;
    if (rv) return true;
    throwdberror(env, jself);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of cursor.
 */
JNIEXPORT jobject JNICALL Java_kyotocabinet_DB_cursor
(JNIEnv* env, jobject jself) {
  try {
    jclass cls_cur = env->FindClass(P_CUR);
    jmethodID id_cur_init = env->GetMethodID(cls_cur, "<init>", "(" L_DB ")V");
    return env->NewObject(cls_cur, id_cur_init, jself);
  } catch (std::exception& e) {
    return NULL;
  }
}


/**
 * Implementation of initialize.
 */
JNIEXPORT void JNICALL Java_kyotocabinet_DB_initialize
(JNIEnv* env, jobject jself, jint opts) {
  try {
    jclass cls_db = env->GetObjectClass(jself);
    jfieldID id_db_ptr = env->GetFieldID(cls_db, "ptr_", "J");
    jfieldID id_db_exbits = env->GetFieldID(cls_db, "exbits_", "I");
    kc::PolyDB* db = new kc::PolyDB();
    int32_t exbits = 0;
    if (opts & GEXCEPTIONAL) {
      exbits |= 1 << kc::PolyDB::Error::NOIMPL;
      exbits |= 1 << kc::PolyDB::Error::INVALID;
      exbits |= 1 << kc::PolyDB::Error::NOREPOS;
      exbits |= 1 << kc::PolyDB::Error::NOPERM;
      exbits |= 1 << kc::PolyDB::Error::BROKEN;
      exbits |= 1 << kc::PolyDB::Error::SYSTEM;
      exbits |= 1 << kc::PolyDB::Error::MISC;
    }
    env->SetLongField(jself, id_db_ptr, (intptr_t)db);
    env->SetIntField(jself, id_db_exbits, exbits);
  } catch (std::exception& e) {}
}


/**
 * Implementation of destruct.
 */
JNIEXPORT void JNICALL Java_kyotocabinet_DB_destruct
(JNIEnv* env, jobject jself) {
  try {
    kc::PolyDB* db = getdbcore(env, jself);
    delete db;
  } catch (std::exception& e) {}
}


/**
 * Implementation of execute.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_MapReduce_execute
(JNIEnv* env, jobject jself, jobject jdb, jstring jtmppath, jint opts) {
  try {
    SoftMapReduce mr(env, jself);
    kc::PolyDB* db = getdbcore(env, jdb);
    SoftString* tmppath = jtmppath ? new SoftString(env, jtmppath) : NULL;
    bool rv = mr.execute(db, tmppath ? tmppath->str() : "", opts);
    delete tmppath;
    jthrowable jex = mr.exception();
    if (jex) {
      env->Throw(jex);
      return false;
    }
    if (rv) return true;
    throwdberror(env, jdb);
    return false;
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of emit.
 */
JNIEXPORT jboolean JNICALL Java_kyotocabinet_MapReduce_emit
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue) {
  try {
    jclass cls_mr = env->GetObjectClass(jself);
    jfieldID id_mr_ptr = env->GetFieldID(cls_mr, "ptr_", "J");
    SoftMapReduce* mr = (SoftMapReduce*)(intptr_t)env->GetLongField(jself, id_mr_ptr);
    SoftArray key(env, jkey);
    SoftArray value(env, jvalue);
    return mr->emit_public(key.ptr(), key.size(), value.ptr(), value.size());
  } catch (std::exception& e) {
    return false;
  }
}


/**
 * Implementation of next.
 */
JNIEXPORT jbyteArray JNICALL Java_kyotocabinet_ValueIterator_next
(JNIEnv* env, jobject jself) {
  try {
    jclass cls_viter = env->GetObjectClass(jself);
    jfieldID id_viter_ptr = env->GetFieldID(cls_viter, "ptr_", "J");
    kc::MapReduce::ValueIterator* viter =
        (kc::MapReduce::ValueIterator*)(intptr_t)env->GetLongField(jself, id_viter_ptr);
    size_t vsiz;
    const char* vbuf = viter->next(&vsiz);
    if (vbuf) return newarray(env, vbuf, vsiz);
    return NULL;
  } catch (std::exception& e) {
    return NULL;
  }
}



// END OF FILE
