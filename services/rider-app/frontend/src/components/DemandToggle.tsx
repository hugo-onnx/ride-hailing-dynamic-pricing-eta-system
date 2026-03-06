interface Props {
  enabled: boolean;
  onToggle: () => void;
}

export default function DemandToggle({ enabled, onToggle }: Props) {
  return (
    <button
      onClick={onToggle}
      className={`absolute top-6 right-6 z-10 w-10 h-10 rounded-full shadow-lg flex items-center justify-center transition-colors ${
        enabled ? 'bg-black text-white' : 'bg-white text-gray-400 hover:text-gray-600'
      }`}
      title={enabled ? 'Hide demand heatmap' : 'Show demand heatmap'}
    >
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
        <path
          d="M9 2C5.134 2 2 5.134 2 9s3.134 7 7 7 7-3.134 7-7-3.134-7-7-7zm0 12.6A5.6 5.6 0 113.4 9 5.607 5.607 0 019 14.6z"
          fill="currentColor"
          opacity="0.5"
        />
        <circle cx="9" cy="9" r="3" fill="currentColor" />
      </svg>
    </button>
  );
}
