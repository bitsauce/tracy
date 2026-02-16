#ifndef __TRACYCONFIG_HPP__
#define __TRACYCONFIG_HPP__

#include "TracyUtility.hpp"

namespace tracy
{

struct Config
{
    bool threadedRendering = true;
    bool focusLostLimit = true;
    int targetFps = 60;
    bool memoryLimit = false;
    int memoryLimitPercent = 80;
    bool achievements = false;
    bool achievementsAsked = false;
    int dynamicColors = 1;
    bool forceColors = false;
    int shortenName = (int)ShortenName::NoSpaceAndNormalize;
    int connectionMode = 0;   // 0 = Connect (outbound TCP), 1 = Host (inbound WS)
    int hostPort = 8086;
    bool hostTls = false;
};

}

#endif
