import ArrowDown from "./icons/triangle-down.png";
import ArrowUp from "./icons/triangle-up.png";

export const comparePerTvlLock = (current, ago) => {
  let aspect = current - ago;
  let compare = 0;
  if (aspect < 0) {
    compare = ((ago - current) / ago) * 100;
    return (
      <>
        <span className="flex items-center text-[#FA3A3A] relative">
          <img src={ArrowDown} alt="" className="w-[10px] h-[10px]" />
          <span className="relative pl-2">{formatNumber(compare)}%</span>
        </span>
      </>
    );
  } else if (aspect === 0) {
    return (
      <span className="flex items-center text-[#737373] relative">
        <span className="relative pl-2">0.00%</span>
      </span>
    );
  } else {
    compare = (aspect / ago) * 100;
    return (
      <span className="flex items-center text-[#4EAB58] relative">
        <img src={ArrowUp} alt="" className="w-[10px] h-[10px]" />
        <span className="relative pl-2">{formatNumber(compare)}%</span>
      </span>
    );
  }
};

export const formatNumber = (num) => {
  return parseFloat(num).toFixed(2);
};

export const numberWithCommas = (x) => {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
};

export const formatPercentage = (x) => {
  return x * 100;
};

export function formatStringToNumber(value, maximumFractionDigits = 2) {
  if (!value && value !== 0) {
    return "-";
  }
  const formatter = new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits,
  });

  return formatter.format(value).replace(/,/g, ",");
}

export const formatTotalVolume = (num) => {
  if (!num && num !== 0) {
    return "-";
  }
  const formatter = (num, maximumFractionDigits = 0) => {
    const formatNum = new Intl.NumberFormat("en-US", {
      minimumFractionDigits: 0,
      maximumFractionDigits,
    });
    return formatNum.format(num).replace(/,/g, ",");
  };
  if (num >= 1000000000) return `${formatter(num / 1000000000, 1)}B`;
  if (num >= 1000000 && num < 1000000000)
    return `${formatter(num / 1000000, 1)}M`;
  if (num >= 1000 && num < 1000000) return `${formatter(num / 1000, 1)}K`;
  if (num >= 1 && num < 1000) return `${formatter(num)}`;
  if (num < 1) return `${formatter(num, 3)}`;
};
