#ifndef _HASH_MAP_H_
#define _HASH_MAPS_H_

#include <iostream>
#include <unordered_map>

template <class _key, class _val>
class SpinLockMap
{
public:
    SpinLockMap(){};
    ~SpinLockMap() { this->map.clear(); }

    bool exists(_key key)
    {
        return this->map.count(key);
    }
    _val get(_key key)
    {
        this->lock.lock();
        assert(this->map.count(key));
        _val temp = this->map[key];
        this->lock.unlock();
        return temp;
    };
    void add(_key key, _val value)
    {
        this->lock.lock();
        this->map[key] = value;
        this->lock.unlock();
    };
    int remove(_key key)
    {
        this->lock.lock();
        int result = this->map.erase(key);
        this->lock.unlock();
        return result;
    }

private:
    std::unordered_map<_key, _val> map;
    std::mutex lock;
};

#endif
