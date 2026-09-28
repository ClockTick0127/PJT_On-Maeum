import { Link } from "react-router-dom";
import mainLogo from "../../assets/main-logo.webp";
import mainTitle from "../../assets/main-title.webp";

export default function Brand() {
  return (
    <Link className="inline-flex items-center gap-2" to="/">
      <img className="h-9 w-9 shrink-0 object-contain" src={mainLogo} alt="" />
      <img className="h-4" src={mainTitle} alt="" />
    </Link>
  );
}
